using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Producers
{
    [ExcludeFromCodeCoverage]
    public class KafkaProducerConfig
    {
        public string BootstrapServers { get; private set; }
        public string Topic { get; private set; }
        public bool? EnableIdempotence { get; private set; }
        public bool? ApiVersionRequest { get; private set; }
        public int MessageSendMaxRetries { get; private set; }
        public Acks? Acks { get; private set; }
        public int? MaxInFlight { get; private set; }

        public void SetBootstrapServers(string bootstrapServers)
        {
            BootstrapServers = bootstrapServers;
        }
        public void SetTopic(string topic)
        {
            Topic = topic;
        }
        public void SetEnableIdempotence(bool enableIdempotence)
        {
            EnableIdempotence = enableIdempotence;
        }
        public void SetApiVersionRequest(bool apiVersionRequest)
        {
            ApiVersionRequest = apiVersionRequest;
        }
        public void SetMessageSendMaxRetries(int messageSendMaxRetries)
        {
            MessageSendMaxRetries = messageSendMaxRetries;
        }
        public void SetAcks(Acks acks)
        {
            Acks = acks;
        }
        public void SetMaxInFlight(int maxInFlight)
        {
            MaxInFlight = maxInFlight;
        }

    }
}
