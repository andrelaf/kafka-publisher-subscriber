using Confluent.Kafka;
using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.PubSub;
using KafkaPublisherSubscriber.Serializers;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Factories
{

    public interface IKafkaFactory
    {
        public KafkaSubConfig SubConfig { get; }
        public KafkaPubConfig PubConfig { get; }
        IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>();
        IProducer<TKey, TValue> CreateProducer<TKey, TValue>();
        Message<TKey, TValue> CreateKafkaMessage<TKey, TValue>(TValue message, TKey key = default!, Headers headers = default!);
    }

    [ExcludeFromCodeCoverage]
    public class KafkaFactory : IKafkaFactory
    {
        private readonly ILogger<IKafkaPubSub> _logger;
        private readonly KafkaSubConfig? _subConfig;
        private readonly KafkaPubConfig? _pubConfig;
        public KafkaFactory(ILogger<IKafkaPubSub> logger, KafkaSubConfig subConfig = default!, KafkaPubConfig? pubConfig = default!)
        {
            _subConfig = subConfig;
            _pubConfig = pubConfig;
            _logger = logger;
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
                AutoOffsetReset = _subConfig.AutoOffsetReset,
                SaslUsername = _subConfig.Username,
                SaslPassword = _subConfig.Password
            };


            if (_subConfig.IsCredentialsProvided)
            {
                consumerConfig.SaslMechanism = SaslMechanism.ScramSha512;
                consumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                consumerConfig.SaslUsername = _subConfig.Username;
                consumerConfig.SaslPassword = _subConfig.Password;
            }

            return new ConsumerBuilder<TKey, TValue>(consumerConfig)
                .SetKeyDeserializer(new JsonDeserializerUtf8<TKey>(_logger))
                .SetValueDeserializer(new JsonDeserializerUtf8<TValue>(_logger))
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

            if (_pubConfig.IsCredentialsProvided)
            {
                producerConfig.SaslMechanism = SaslMechanism.ScramSha512;
                producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                producerConfig.SaslUsername = _pubConfig.Username;
                producerConfig.SaslPassword = _pubConfig.Password;
            }

            return new ProducerBuilder<TKey, TValue>(producerConfig)
                .SetKeySerializer(new JsonSerializerUtf8<TKey>())
                .SetValueSerializer(new JsonSerializerUtf8<TValue>())
                .Build();
        }


        public Message<TKey, TValue> CreateKafkaMessage<TKey, TValue>(TValue message, TKey key = default!, Headers headers = default!)
        {
            return new Message<TKey, TValue>
            {
                Key = key,
                Value = message,
                Headers = headers
            };
        }
    }
}