using System;
using Microsoft.Extensions.DependencyInjection;
using SlimMessageBus.Host.DependencyResolver;

namespace SlimMessageBus.Host.AspNetCore
{
    internal class MsDependencyInjectionChildDependencyResolver : IDependencyResolver
    {
        private readonly IServiceScope serviceProvider;

        public MsDependencyInjectionChildDependencyResolver(IServiceScope serviceProvider) 
            => this.serviceProvider = serviceProvider;

        public IDependencyResolver CreateChildScope() 
            => new MsDependencyInjectionChildDependencyResolver(serviceProvider.ServiceProvider.CreateScope());

        public void Dispose() 
            => serviceProvider.Dispose();

        public object Resolve(Type type) 
            => serviceProvider.ServiceProvider.GetService(type);
    }
}
