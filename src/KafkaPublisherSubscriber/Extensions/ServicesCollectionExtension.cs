using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.PubSub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace KafkaPublisherSubscriber.Extensions
{
    public static class ServicesCollectionExtension
    {
        public static IServiceCollection AddKafkaPubSub<TKey, TValue>(this IServiceCollection services,
                                                                           Action<KafkaSubConfig> subConfigAction = default!,
                                                                           Action<KafkaPubConfig> pubConfigAction = default!)
        {
            return services.AddKafka<IKafkaPubSub<TKey, TValue>, KafkaPubSub<TKey, TValue>>(subConfigAction, pubConfigAction);
        }

        private static IServiceCollection AddKafka<TService, TImplementation>(this IServiceCollection services,
                                                                             Action<KafkaSubConfig> subConfigAction = default!,
                                                                            Action<KafkaPubConfig> pubConfigAction = default!)
       where TService : class, IKafkaPubSub
       where TImplementation : class, TService
        {
            if (subConfigAction is null && pubConfigAction is null)
            {
                throw new ArgumentException($"{nameof(subConfigAction)} and/or {nameof(pubConfigAction)} are required.");
            }


            KafkaSubConfig? subConfig = null;
            if (subConfigAction is not null)
            {
                subConfig = new KafkaSubConfig();
                subConfigAction.Invoke(subConfig);
                KafkaValidatorConfig.ValidateSubConfig(subConfig);
            }

            KafkaPubConfig? pubConfig = null;
            if (pubConfigAction != null)
            {
                pubConfig = new KafkaPubConfig();
                pubConfigAction.Invoke(pubConfig);
                KafkaValidatorConfig.ValidatePubConfig(pubConfig);
            }

            var boorstrapServers = (pubConfig?.BootstrapServers ?? subConfig?.BootstrapServers);
            var username = (pubConfig?.Username?? subConfig?.Username);
            var password = (pubConfig?.Password ?? subConfig?.Username);

            services.AddKafkaHealthCheck(boorstrapServers, username, password);

            services.AddSingleton<TService, TImplementation>(s =>
            {

                var logger = s.GetRequiredService<ILogger<IKafkaPubSub>>();
                return (TImplementation)Activator.CreateInstance(typeof(TImplementation), new KafkaFactory(logger, subConfig!, pubConfig!))!;
            });
            return services;


        }

        private static IServiceCollection AddKafkaHealthCheck(this IServiceCollection services, string boorstrapServers, string username, string password)
        {

            // Register HealthCheck
            return services;
        }



    }
}
