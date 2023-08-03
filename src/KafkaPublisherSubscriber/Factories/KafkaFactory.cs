using Confluent.Kafka;
using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Serializers;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Factories
{

    public interface IKafkaFactory
    {
        public KafkaSubConfig SubConfig { get; }
        public KafkaPubConfig PubConfig { get; }
        IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>();
        IProducer<TKey, TValue> CreateProducer<TKey, TValue>();
    }

    [ExcludeFromCodeCoverage]
    public class KafkaFactory : IKafkaFactory
    {
        private readonly KafkaSubConfig? _subConfig;
        private readonly KafkaPubConfig? _pubConfig;
        public KafkaFactory(KafkaSubConfig? subConfig = default!, KafkaPubConfig? pubConfig = default!)
        {
            _subConfig = subConfig;
            _pubConfig = pubConfig;
        }

        public KafkaSubConfig SubConfig => _subConfig!;
        public KafkaPubConfig PubConfig => _pubConfig!;

        public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>()
        {
            ArgumentNullException.ThrowIfNull(_subConfig);

            var consumerConfig = new ConsumerConfig()
            {
                GroupId = _subConfig.GroupId,
                BootstrapServers = _subConfig.BootstrapServers,
                EnableAutoCommit = _subConfig.EnableAutoCommit,
                StatisticsIntervalMs = _subConfig.StatisticsIntervalMs,
                SessionTimeoutMs = _subConfig.SessionTimeoutMs,
                EnablePartitionEof = _subConfig.EnablePartitionEof,
                ApiVersionRequest = _subConfig.ApiVersionRequest,
                AutoOffsetReset = _subConfig.AutoOffsetReset
            };

            return new ConsumerBuilder<TKey, TValue>(consumerConfig)
                .SetKeyDeserializer(new JsonDeserializerUtf8<TKey>())
                .SetValueDeserializer(new JsonDeserializerUtf8<TValue>())
                .Build();
        }

        public IProducer<TKey, TValue> CreateProducer<TKey, TValue>()
        {

            ArgumentNullException.ThrowIfNull(_pubConfig);

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = _pubConfig.BootstrapServers,
                EnableIdempotence = _pubConfig.EnableIdempotence,
                Acks = _pubConfig.Acks,
                MaxInFlight = _pubConfig.MaxInFlight,
                MessageSendMaxRetries = _pubConfig.MessageSendMaxRetries,
                ApiVersionRequest = _pubConfig.ApiVersionRequest,
            };

            return new ProducerBuilder<TKey, TValue>(producerConfig)
                .SetKeySerializer(new JsonSerializerUtf8<TKey>())
                .SetValueSerializer(new JsonSerializerUtf8<TValue>())
                .Build();
        }
    }
}