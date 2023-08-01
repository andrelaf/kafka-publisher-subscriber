using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Handlers;
using KafkaPublisherSubscriber.Producers;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace KafkaPublisherSubscriber
{
    public static class DependencyInjectionConfig
    {
        public static IServiceCollection AddKafkaProducer(this IServiceCollection services, Action<KafkaProducerSettingsBuilder> producerConfigAction)
        {
            services.AddSingleton<IKafkaProducer>(KafkaProducer.CreateInstance(producerConfigAction));
            return services;
        }

        public static IServiceCollection AddKafkaConsumer(this IServiceCollection services, Action<KafkaConsumerSettingsBuilder> consumerConfigAction)
        {
            services.AddSingleton<IKafkaConsumer>(KafkaConsumer.CreateInstance(consumerConfigAction));
            return services;
        }

        public static IServiceCollection AddKafkaProducerAndConsumer(this IServiceCollection services, Action<KafkaConsumerSettingsBuilder> consumerConfigAction, Action<KafkaProducerSettingsBuilder> producerConfigAction)
        {
            services.AddSingleton<IKafkaConsumer>(KafkaConsumer.CreateInstance(consumerConfigAction));
            services.AddSingleton<IKafkaProducer>(KafkaProducer.CreateInstance(producerConfigAction));
            services.AddSingleton<IKafkaMessageHandler, KafkaMessageHandler>();
            return services;
        }

    }
}
