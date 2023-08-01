using Confluent.Kafka;
using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Producers;
using System;

namespace KafkaPublisherSubscriber.Factories
{
    public static class KafkaConnectionFactory
    {
        public static IConsumer<Ignore, string> CreateConsumer(KafkaConsumerSettings settings)
        {
            if (settings is null)
            {
                throw new ArgumentNullException(nameof(KafkaConsumerSettings));
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

            return new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        }

        public static IProducer<Null, string> CreateProducer(KafkaProducerSettings settings)
        {
            if (settings is null)
            {
                throw new ArgumentNullException(nameof(KafkaProducerSettings));
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

            return new ProducerBuilder<Null, string>(producerConfig).Build();
        }
    }
}
