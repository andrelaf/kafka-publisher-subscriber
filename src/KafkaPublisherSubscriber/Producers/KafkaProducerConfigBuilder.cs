using Confluent.Kafka;
using System;

namespace KafkaPublisherSubscriber.Producers
{
    public class KafkaProducerConfigBuilder
    {
        private readonly KafkaProducerConfig settings = new KafkaProducerConfig();

        public KafkaProducerConfigBuilder WithBootstrapServers(string bootstrapServers)
        {
            settings.SetBootstrapServers(bootstrapServers);
            return this;
        }

        public KafkaProducerConfigBuilder WithTopic(string topic)
        {
            settings.SetTopic(topic);
            return this;
        }

        public KafkaProducerConfigBuilder WithEnableIdempotence(bool enableIdempotence)
        {
            settings.SetEnableIdempotence(enableIdempotence);
            return this;
        }

        public KafkaProducerConfigBuilder WithApiVersionRequest(bool apiVersionRequest)
        {
            settings.SetApiVersionRequest(apiVersionRequest);
            return this;
        }

        public KafkaProducerConfigBuilder WithMessageSendMaxRetries(int messageSendMaxRetries)
        {
            settings.SetMessageSendMaxRetries(messageSendMaxRetries);
            return this;
        }

        public KafkaProducerConfigBuilder WithAcks(Acks acks)
        {
            settings.SetAcks(acks);
            return this;
        }

        public KafkaProducerConfigBuilder WithMaxInFlight(int maxInFlight)
        {
            settings.SetMaxInFlight(maxInFlight);
            return this;
        }

        public KafkaProducerConfig Build()
        {
            if (settings.BootstrapServers is null || settings.Topic is null)
            {
                throw new ArgumentNullException($"Arguments required {nameof(settings.BootstrapServers)}, {nameof(settings.Topic)}.");
            }

            return settings;
        }
    }
}
