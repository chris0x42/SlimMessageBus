using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SlimMessageBus.Host.Config;

namespace SlimMessageBus.Host
{
    /// <summary>
    /// Represents a pool of consumer instance that compete to handle a message.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class ConsumerInstancePoolMessageProcessor<TMessage> : IMessageProcessor<TMessage> where TMessage : class
    {
        private readonly ILogger _logger;

        private readonly MessageBusBase _messageBus;
        private readonly ConsumerSettings _consumerSettings;

        private readonly Func<TMessage, byte[]> _messagePayloadProvider;

        private readonly bool _consumerWithContext;
        private readonly Action<TMessage, ConsumerContext> _consumerContextInitializer;
        private readonly SemaphoreSlim _concurrentInstancesSemaphore;
        private readonly bool _createMessageScope;

        public ConsumerInstancePoolMessageProcessor(ConsumerSettings consumerSettings, MessageBusBase messageBus, Func<TMessage, byte[]> messagePayloadProvider, Action<TMessage, ConsumerContext> consumerContextInitializer = null)
        {
            if (messageBus is null) throw new ArgumentNullException(nameof(messageBus));

            _logger = messageBus.LoggerFactory.CreateLogger<ConsumerInstancePoolMessageProcessor<TMessage>>();
            _consumerSettings = consumerSettings ?? throw new ArgumentNullException(nameof(consumerSettings));
            _messageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
            _messagePayloadProvider = messagePayloadProvider ?? throw new ArgumentNullException(nameof(messagePayloadProvider));

            _createMessageScope = _consumerSettings.IsMessageScopeEnabled ?? _messageBus.Settings.IsMessageScopeEnabledDefault;
            _consumerContextInitializer = consumerContextInitializer;
            _consumerWithContext = typeof(IConsumerContextAware).IsAssignableFrom(consumerSettings.ConsumerType);

            _concurrentInstancesSemaphore = new SemaphoreSlim(0, _consumerSettings.Instances);
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _concurrentInstancesSemaphore.Dispose();
            }
        }

        #endregion

        public virtual async Task<Exception> ProcessMessage(TMessage msg)
        {
            Exception exceptionResult = null;
            try
            {
                DeserializeMessage(msg, out var requestMessage, out var requestId, out var expires, out var message);

                // Verify if the request/message is already expired
                if (expires != null)
                {
                    var currentTime = _messageBus.CurrentTime;
                    if (currentTime > expires.Value)
                    {
                        OnMessageExpired(expires, message, currentTime);

                        // Do not process the expired message
                        return null;
                    }
                }

                object response = null;
                string responseError = null;

                // Ensure only desired number of messages are being processed concurrently
                await _concurrentInstancesSemaphore.WaitAsync().ConfigureAwait(false);
                try
                {
                    var messageScope = _createMessageScope
                        ? _messageBus.Settings.DependencyResolver.CreateScope()
                        : _messageBus.Settings.DependencyResolver;
                    try
                    {
                        // Set MessageScope.Current, so any future integration might need to use that
                        MessageScope.SetCurrent(messageScope);

                        var consumerInstance = messageScope.Resolve(_consumerSettings.ConsumerType);
                        try
                        {
                            response = await ExecuteConsumer(msg, message, response, consumerInstance).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            responseError = OnMessageError(message, e);
                            exceptionResult = e;
                        }
                        finally
                        {
                            if (_consumerSettings.IsDisposeConsumerEnabled && consumerInstance is IDisposable consumerInstanceDisposable)
                            {
                                consumerInstanceDisposable.DisposeSilently("ConsumerInstance", _logger);
                            }
                        }

                        // Clear the MessageScope.Current
                        MessageScope.SetCurrent(null);
                    }
                    finally
                    {
                        if (_createMessageScope)
                        {
                            messageScope.DisposeSilently("Scope", _logger);
                        }
                    }
                }
                finally
                {
                    _concurrentInstancesSemaphore.Release();
                }

                if (response != null || responseError != null)
                {
                    await ProduceResponse(requestMessage, requestId, message, response, responseError).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Processing of the message {message} of type {consumerType} failed", msg, _consumerSettings.MessageType);
                exceptionResult = e;

            }
            return exceptionResult;
        }

        private void OnMessageExpired(DateTimeOffset? expires, object message, DateTimeOffset currentTime)
        {
            _logger.LogWarning("The message arrived too late and is already expired (expires {0}, current {1})", expires.Value, currentTime);

            try
            {
                // Execute the event hook
                _consumerSettings.OnMessageExpired?.Invoke(_messageBus, _consumerSettings, message);
                _messageBus.Settings.OnMessageExpired?.Invoke(_messageBus, _consumerSettings, message);
            }
#pragma warning disable CA1031 // Intended, a catch all situation
            catch (Exception eh)
#pragma warning restore CA1031 
            {
                MessageBusBase.HookFailed(_logger, eh, nameof(IConsumerEvents.OnMessageExpired));
            }
        }

        private string OnMessageError(object message, Exception e)
        {
            string responseError = null;

            if (_consumerSettings.ConsumerMode == ConsumerMode.RequestResponse)
            {
                _logger.LogError(e, "Handler execution failed");
                // Save the exception
                responseError = e.ToString();
            }
            else
            {
                _logger.LogError(e, "Consumer execution failed");
            }

            try
            {
                // Execute the event hook
                _consumerSettings.OnMessageFault?.Invoke(_messageBus, _consumerSettings, message, e);
                _messageBus.Settings.OnMessageFault?.Invoke(_messageBus, _consumerSettings, message, e);
            }
#pragma warning disable CA1031 // Intended, a catch all situation
            catch (Exception eh)
#pragma warning restore CA1031
            {
                MessageBusBase.HookFailed(_logger, eh, nameof(IConsumerEvents.OnMessageFault));
            }

            return responseError;
        }

        private void OnMessageArrived(object message)
        {
            try
            {
                // Execute the event hook
                _consumerSettings.OnMessageArrived?.Invoke(_messageBus, _consumerSettings, message, _consumerSettings.Topic);
                _messageBus.Settings.OnMessageArrived?.Invoke(_messageBus, _consumerSettings, message, _consumerSettings.Topic);
            }
#pragma warning disable CA1031 // Intended, a catch all situation
            catch (Exception eh)
#pragma warning restore CA1031
            {
                MessageBusBase.HookFailed(_logger, eh, nameof(IConsumerEvents.OnMessageArrived));
            }
        }

        private async Task<object> ExecuteConsumer(TMessage msg, object message, object response, object consumerInstance)
        {
            if (_consumerWithContext && _consumerContextInitializer != null)
            {
                var consumerContext = new ConsumerContext();
                _consumerContextInitializer(msg, consumerContext);

                var consumerWithContext = (IConsumerContextAware)consumerInstance;
                consumerWithContext.Context.Value = consumerContext;
            }

            OnMessageArrived(message);

            // the consumer just subscribes to the message
            var task = _consumerSettings.ConsumerMethod(consumerInstance, message, _consumerSettings.Topic);
            await task.ConfigureAwait(false);

            if (_consumerSettings.ConsumerMode == ConsumerMode.RequestResponse)
            {
                // the consumer handles the request (and replies)
                response = _consumerSettings.ConsumerMethodResult(task);
            }

            return response;
        }

        protected void DeserializeMessage(TMessage msg, out MessageWithHeaders requestMessage, out string requestId, out DateTimeOffset? expires, out object message)
        {
            var msgPayload = _messagePayloadProvider(msg);

            requestMessage = null;
            requestId = null;
            expires = null;

            _logger.LogDebug("Deserializing message...");

            message = _consumerSettings.IsRequestMessage
                ? _messageBus.DeserializeRequest(_consumerSettings.MessageType, msgPayload, out requestMessage)
                : _messageBus.Settings.Serializer.Deserialize(_consumerSettings.MessageType, msgPayload);

            if (requestMessage != null)
            {
                requestMessage.TryGetHeader(ReqRespMessageHeaders.RequestId, out requestId);
                requestMessage.TryGetHeader(ReqRespMessageHeaders.Expires, out expires);
            }
        }

        private async Task ProduceResponse(MessageWithHeaders requestMessage, string requestId, object message, object response, string responseError)
        {
            // send the response (or error response)
            _logger.LogDebug("Serializing the response {0} of type {1} for RequestId: {2}...", response, _consumerSettings.ResponseType, requestId);

            var responseMessage = new MessageWithHeaders();
            responseMessage.SetHeader(ReqRespMessageHeaders.RequestId, requestId);
            responseMessage.SetHeader(ReqRespMessageHeaders.Error, responseError);

            await _messageBus.ProduceResponse(message, requestMessage, response, responseMessage, _consumerSettings).ConfigureAwait(false);
        }

    }
}