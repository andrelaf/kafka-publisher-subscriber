using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Configs;

[ExcludeFromCodeCoverage]
public sealed class KafkaSubConfig : KafkaConfig
{
    public string? GroupId { get; private set; }
    public int? StatisticsIntervalMs { get; private set; } = 10000;
    public int? SessionTimeoutMs { get; private set; } = 10000;
    public bool EnableAutoCommit { get; private set; } = false;
    public bool EnablePartitionEof { get; private set; } = true;
    public bool EnableRetryTopicSubscription { get; private set; } = false;
    public bool? ApiVersionRequest { get; private set; }
    public string? TopicRetry { get; private set; }
    public string? TopicDeadLetter { get; private set; }
    public AutoOffsetReset AutoOffsetReset { get; private set; } = AutoOffsetReset.Latest;
    public int RetryLimit { get; private set; } = 3;
    public int DelayIPartitionEofMs { get; private set; } = 1000;
    public int MaxConcurrentMessages { get; private set; } = 0;
    public double MessageProcessingTimeoutMs { get; private set; } = 1000;

    public void SetGroupId(string groupId)
    {
        GroupId = groupId;
    }
    public void SetAutoCommitEnabled()
    {
        EnableAutoCommit = true;
    }
    public void SetStatisticsIntervalMs(int statisticsIntervalMs)
    {
        StatisticsIntervalMs = statisticsIntervalMs;
    }
    public void SetSessionTimeoutMs(int sessionTimeoutMs)
    {
        SessionTimeoutMs = sessionTimeoutMs;
    }
    public void SetRetryTopicSubscriptionEnabled()
    {
        EnableRetryTopicSubscription = true;
    }
    public void SetApiVersionRequest(bool apiVersionRequest)
    {
        ApiVersionRequest = apiVersionRequest;
    }
    public void SetTopicRetry(string topicRetry)
    {
        TopicRetry = topicRetry;
    }
    public void SetTopicDeadLetter(string topicDeadLetter)
    {
        TopicDeadLetter = topicDeadLetter;
    }
    public void SetAutoOffsetReset(AutoOffsetReset autoOffsetReset)
    {
        AutoOffsetReset = autoOffsetReset;
    }
    /// <summary>
    /// Define o limite de tentativas de processamento de uma mensagem. Se a mensagem falhar ao ser processada 
    /// após exceder esse limite, ela não será mais retentada.
    /// </summary>
    /// <param name="retryLimit">O número máximo de tentativas de processamento da mensagem.</param>
    public void SetRetryLimit(int retryLimit)
    {
        RetryLimit = retryLimit;
    }
    public void SetDelayIPartitionEofMs(int delayIPartitionEofMs)
    {
        DelayIPartitionEofMs = delayIPartitionEofMs;
    }  
    public void SetMessageProcessingTimeoutMs(int messageProcessingTimeoutMs)
    {
        MessageProcessingTimeoutMs = messageProcessingTimeoutMs;
    }
}
