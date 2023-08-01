using Confluent.Kafka;
using System;

namespace KafkaPublisherSubscriber.Producers
{
    public class KafkaProducerSettingsBuilder
    {
        private readonly KafkaProducerSettings settings = new KafkaProducerSettings();

        public KafkaProducerSettingsBuilder WithBootstrapServers(string bootstrapServers)
        {
            settings.SetBootstrapServers(bootstrapServers);
            return this;
        }

        public KafkaProducerSettingsBuilder WithTopic(string topic)
        {
            settings.SetTopic(topic);
            return this;
        }

        public KafkaProducerSettingsBuilder WithEnableIdempotence(bool enableIdempotence)
        {
            settings.SetEnableIdempotence(enableIdempotence);
            return this;
        }

        public KafkaProducerSettingsBuilder WithApiVersionRequest(bool apiVersionRequest)
        {
            settings.SetApiVersionRequest(apiVersionRequest);
            return this;
        }

        public KafkaProducerSettingsBuilder WithMessageSendMaxRetries(int messageSendMaxRetries)
        {
            settings.SetMessageSendMaxRetries(messageSendMaxRetries);
            return this;
        }

        public KafkaProducerSettingsBuilder WithAcks(Acks acks)
        {
            settings.SetAcks(acks);
            return this;
        }

        public KafkaProducerSettingsBuilder WithMaxInFlight(int maxInFlight)
        {
            settings.SetMaxInFlight(maxInFlight);
            return this;
        }

        public KafkaProducerSettings Build()
        {
            if (settings.BootstrapServers is null || settings.Topic is null)
            {
                throw new ArgumentNullException($"Arguments required {nameof(settings.BootstrapServers)}, {nameof(settings.Topic)}.");
            }

            return settings;
        }
    }
}
