using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Handlers;
using KafkaPublisherSubscriber.Producers;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace KafkaPublisherSubscriber
{
    public static class DependencyInjectionConfig
    {
        public static IServiceCollection AddKafkaProducer<TKey, TValue>(this IServiceCollection services, Action<KafkaProducerConfigBuilder> producerConfigAction)
        {
            services.AddSingleton<IKafkaProducer<TKey, TValue>>(provider =>
            {;
                return KafkaProducer<TKey, TValue>.CreateInstance(producerConfigAction);
            });

            return services;
        }

        public static IServiceCollection AddKafkaConsumer<TKey, TValue>(this IServiceCollection services, Action<KafkaConsumerConfigBuilder> consumerConfigAction)
        {
            services.AddSingleton<IKafkaConsumer<TKey, TValue>>(provider =>
            {
                return KafkaConsumer<TKey, TValue>.CreateInstance(consumerConfigAction);
            });

            return services;
        }

        public static IServiceCollection AddKafkaProducerAndConsumer<TKey, TValue>(this IServiceCollection services, Action<KafkaConsumerConfigBuilder> consumerConfigAction, Action<KafkaProducerConfigBuilder> producerConfigAction)
        {
            services.AddSingleton<IKafkaConsumer<TKey, TValue>>(provider =>
            {
                return KafkaConsumer<TKey, TValue>.CreateInstance(consumerConfigAction);
            });

            services.AddSingleton<IKafkaProducer<TKey, TValue>>(provider =>
            {
                return KafkaProducer<TKey, TValue>.CreateInstance(producerConfigAction);
            });

            services.AddSingleton<IKafkaMessageHandler<TKey, TValue>, KafkaMessageHandler<TKey, TValue>>();
            return services;
        }

    }
}
