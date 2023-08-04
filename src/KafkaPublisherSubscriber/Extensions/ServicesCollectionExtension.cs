using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.PubSub;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPublisherSubscriber.Extensions
{
    public static class ServicesCollectionExtension
    {
        public static IServiceCollection AddKafkaPubSub<TService, TImplementation>(this IServiceCollection services,
                                                                                     Action<KafkaSubConfig> subConfigAction = default!,
                                                                                     Action<KafkaPubConfig> pubConfigAction = default!)
            where TService : class, IKafkaPubSub
            where TImplementation : class, TService
        {
            if(subConfigAction is null && pubConfigAction is null)
            {
                throw new ArgumentException($"{nameof(subConfigAction)} and/or {nameof(pubConfigAction)} are required.");
            }


            KafkaSubConfig? subConfig = null;
            if (subConfigAction is not null)
            {
                subConfig = new KafkaSubConfig();
                subConfigAction.Invoke(subConfig);
                KafkaConfigValidator.ValidateSubConfig(subConfig);
            }
           
            KafkaPubConfig? pubConfig = null;
            if (pubConfigAction != null)
            {
                pubConfig = new KafkaPubConfig();
                pubConfigAction.Invoke(pubConfig);
                KafkaConfigValidator.ValidatePubConfig(pubConfig);
            }

            services.AddScoped<TService, TImplementation>(s => (TImplementation)Activator.CreateInstance(typeof(TImplementation), subConfig, pubConfig)!);
            return services;
        }

    }
}
