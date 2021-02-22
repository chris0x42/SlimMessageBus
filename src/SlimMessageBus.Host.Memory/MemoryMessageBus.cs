using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host.Memory
{
    /// <summary>
    /// In-memory message bus <see cref="IMessageBus"/> implementation to use for in process message passing.
    /// </summary>
    public class MemoryMessageBus : MessageBusBase
    {
        private readonly ILogger _logger;

        private MemoryMessageBusSettings ProviderSettings { get; }

        private IDictionary<string, List<ConsumerSettings>> _consumersByTopic;

        public MemoryMessageBus(MessageBusSettings settings, MemoryMessageBusSettings providerSettings) : base(settings)
        {
            _logger = LoggerFactory.CreateLogger<MemoryMessageBus>();
            ProviderSettings = providerSettings ?? throw new ArgumentNullException(nameof(providerSettings));

            OnBuildProvider();
        }

        #region Overrides of MessageBusBase

        protected override void AssertSerializerSettings()
        {
            if (ProviderSettings.EnableMessageSerialization)
            {
                base.AssertSerializerSettings();
            }
        }

        protected override void Build()
        {
            base.Build();

            _consumersByTopic = Settings.Consumers
                .GroupBy(x => x.Topic)
                .ToDictionary(x => x.Key, x => x.ToList());
        }

        public override Task ProduceToTransport(Type messageType, object message, string name, byte[] messagePayload, MessageWithHeaders messageWithHeaders = null)
        {
            if (!_consumersByTopic.TryGetValue(name, out var consumers))
            {
                _logger.LogDebug("No consumers interested in message type {messageType} on topic {topic}", messageType, name);
                return Task.CompletedTask;
            }

            var tasks = new LinkedList<Task>();
            foreach (var consumer in consumers)
            {
                var task = OnMessageProduced(messageType, message, name, messagePayload, messageWithHeaders, consumer);
                if (task != null)
                {
                    tasks.AddLast(task);
                }
            }

            _logger.LogDebug("Waiting on {0} consumer tasks", tasks.Count);
            return Task.WhenAll(tasks);
        }

        private async Task OnMessageProduced(Type messageType, object message, string name, byte[] messagePayload, MessageWithHeaders messageWithHeaders, ConsumerSettings consumer)
        {
            // ToDo: Extension: In case of IMessageBus.Publish do not wait for the consumer method see https://github.com/zarusz/SlimMessageBus/issues/37

            string responseError = null;
            Task consumerTask = null;

            try
            {
                consumerTask = ExecuteConsumer(messageType, message, messagePayload, consumer);
                await consumerTask.ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Intended, a catch all situation
            catch (Exception e)
#pragma warning restore CA1031
            {
                responseError = e.Message;
            }

            if (consumer.ConsumerMode == ConsumerMode.RequestResponse)
            {
                var requestId = messageWithHeaders.Headers[ReqRespMessageHeaders.RequestId];

                if (responseError != null)
                {
                    await OnResponseArrived(null, name, requestId, responseError, null).ConfigureAwait(false);
                }
                else
                {
                    var response = consumer.ConsumerMethodResult(consumerTask);
                    var responsePayload = SerializeMessage(consumer.ResponseType, response);

                    await OnResponseArrived(responsePayload, name, requestId, null, response).ConfigureAwait(false);
                }
            }
        }

        private async Task ExecuteConsumer(Type messageType, object message, byte[] messagePayload, ConsumerSettings consumerSettings)
        {
            var createMessageScope = IsMessageScopeEnabled(consumerSettings);
            var messageScope = createMessageScope
                ? Settings.DependencyResolver.CreateScope()
                : Settings.DependencyResolver;

            try
            {
                MessageScope.Current = messageScope;

                // obtain the consumer from DI
                _logger.LogDebug("Resolving consumer type {consumerType}", consumerSettings.ConsumerType);
                var consumerInstance = messageScope.Resolve(consumerSettings.ConsumerType);
                if (consumerInstance == null)
                {
                    throw new ConfigurationMessageBusException($"The dependency resolver does not know how to create an instance of {consumerSettings.ConsumerType}");
                }

                try
                {
                    var messageForConsumer = !ProviderSettings.EnableMessageSerialization
                        ? message // prevent deep copy of the message
                        : consumerSettings.ConsumerMode == ConsumerMode.RequestResponse
                            ? DeserializeRequest(messageType, messagePayload, out var _) // will pass a deep copy of the message
                            : DeserializeMessage(messageType, messagePayload); // will pass a deep copy of the message

                    _logger.LogDebug("Invoking {0} {1}", consumerSettings.ConsumerMode == ConsumerMode.Consumer ? "consumer" : "handler", consumerInstance.GetType());
                    await consumerSettings.ConsumerMethod(consumerInstance, messageForConsumer, consumerSettings.Topic).ConfigureAwait(false);
                }
                finally
                {
                    if (consumerSettings.IsDisposeConsumerEnabled && consumerInstance is IDisposable consumerInstanceDisposable)
                    {
                        consumerInstanceDisposable.DisposeSilently("ConsumerInstance", _logger);
                    }
                }

                MessageScope.Current = null;
            }
            finally
            {
                if (createMessageScope)
                {
                    messageScope.DisposeSilently("Scope", _logger);
                }
            }
        }

        public override byte[] SerializeMessage(Type messageType, object message)
        {
            if (!ProviderSettings.EnableMessageSerialization)
            {
                // the serialized payload is not going to be used
                return null;
            }

            return base.SerializeMessage(messageType, message);
        }

        public override byte[] SerializeRequest(Type requestType, object request, MessageWithHeaders requestMessage, ProducerSettings producerSettings)
        {
            if (!ProviderSettings.EnableMessageSerialization)
            {
                // the serialized payload is not going to be used
                return null;
            }

            return base.SerializeRequest(requestType, request, requestMessage, producerSettings);
        }

        public override byte[] SerializeResponse(Type responseType, object response, MessageWithHeaders responseMessage)
        {
            if (!ProviderSettings.EnableMessageSerialization)
            {
                // the serialized payload is not going to be used
                return null;
            }

            return base.SerializeResponse(responseType, response, responseMessage);
        }

        #endregion

        public override bool IsMessageScopeEnabled(ConsumerSettings consumerSettings)
            => consumerSettings.IsMessageScopeEnabled ?? Settings.IsMessageScopeEnabled ?? false; // by default Memory Bus has scoped message disabled
    }
}
