namespace KafkaPublisherSubscriber.Settings;

public class KafkaBrokerSettings
{
    public const string KeyNameSettings = "KafkaBrokerSettings";
    public required string BootstrapServers { get; set; }
    public required List<KafkaPubSubSettings> PubSubSettings { get; set; }

}

public class KafkaPubSubSettings
{
    public required string Topic { get; set; }
    public string? TopicRetry { get; set; }
    public string? TopicDeadLetter { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? ConsumerGroupId { get; set; }
    public int? MessageSendMaxRetries { get; set; }
    public int? MaxInFlight { get; private set; }
    public int? RetryLimit { get; set; }
    public int? DelayInSecondsPartitionEof { get; set; }
    public int? MessageProcessingTimeoutMilliseconds { get; set; }

}
