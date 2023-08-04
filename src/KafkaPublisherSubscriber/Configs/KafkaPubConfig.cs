using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Configs
{
    [ExcludeFromCodeCoverage]
    public sealed class KafkaPubConfig
    {
        public string? BootstrapServers { get; private set; }
        public string? Topic { get; private set; }
        public bool EnableIdempotence { get; private set; } = false;
        public bool? ApiVersionRequest { get; private set; }
        public int MessageSendMaxRetries { get; private set; } = 10;
        public Acks Acks { get; private set; } = Acks.Leader;
        public int? MaxInFlight { get; private set; }

        public void SetBootstrapServers(string bootstrapServers)
        {
            BootstrapServers = bootstrapServers;
        }
        public void SetTopic(string topic)
        {
            Topic = topic;
        }
        public void SetIdempotenceEnabled()
        {
            EnableIdempotence = true;
            MaxInFlight = 5;
            MessageSendMaxRetries = 3;
            Acks = Acks.All;
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
