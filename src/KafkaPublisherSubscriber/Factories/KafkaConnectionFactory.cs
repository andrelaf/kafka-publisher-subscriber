using Confluent.Kafka;
using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Producers;
using System;

namespace KafkaPublisherSubscriber.Factories
{
    public static class KafkaConnectionFactory
    {
        public static IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(KafkaConsumerConfig settings)
        {
            if (settings is null)
            {
                throw new ArgumentNullException(nameof(KafkaConsumerConfig));
            }

            var consumerConfig = new ConsumerConfig()
            {
                GroupId = settings.GroupId,
                BootstrapServers = settings.BootstrapServers,
                EnableAutoCommit = settings.EnableAutoCommit,
                StatisticsIntervalMs = settings.StatisticsIntervalMs,
                SessionTimeoutMs = settings.SessionTimeoutMs,
                EnablePartitionEof = settings.EnablePartitionEof,
                ApiVersionRequest = settings.ApiVersionRequest,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            return new ConsumerBuilder<TKey, TValue>(consumerConfig).Build();
        }

        public static IProducer<TKey, TValue> CreateProducer<TKey, TValue>(KafkaProducerConfig settings)
        {
            if (settings is null)
            {
                throw new ArgumentNullException(nameof(KafkaProducerConfig));
            }

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = settings.BootstrapServers,
                EnableIdempotence = settings.EnableIdempotence,
                Acks = settings.Acks,
                MaxInFlight = settings.MaxInFlight,
                MessageSendMaxRetries = settings.MessageSendMaxRetries,
                ApiVersionRequest = settings.ApiVersionRequest,
            };

            return new ProducerBuilder<TKey, TValue>(producerConfig).Build();
        }
    }

}