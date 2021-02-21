using SlimMessageBus.Host.DependencyResolver;
using System.Threading;

namespace SlimMessageBus.Host
{
    public static class MessageScope
    {
        private static readonly AsyncLocal<IDependencyResolver> _current = new AsyncLocal<IDependencyResolver>();

        public static IDependencyResolver Current
        {
            get => _current.Value;
            internal set => _current.Value = value;
        }
    }
}
