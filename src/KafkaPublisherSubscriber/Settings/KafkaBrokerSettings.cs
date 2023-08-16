using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Settings;

[ExcludeFromCodeCoverage]
public class KafkaBrokerSettings
{
    public required string BootstrapServers { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }
    public required Dictionary<string, KafkaTopicSettings> Topics { get; set; }

}

public class KafkaTopicSettings
{
    public string? TopicRetry { get; set; }
    public string? TopicDeadLetter { get; set; }
    public int MessageSendMaxRetries { get; set; }
    public int MaxInFlight { get; set; }
    public string? GroupId { get; set; }
    public int RetryLimit { get; set; }
    public int DelayPartitionEofMs { get; set; }
    public int MaxConcurrentMessages { get; set; } 
    public int MessageProcessingTimeoutMilliseconds { get; set; }

}
