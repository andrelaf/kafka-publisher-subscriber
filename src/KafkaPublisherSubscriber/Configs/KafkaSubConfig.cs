using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Configs
{
    [ExcludeFromCodeCoverage]
    public sealed class KafkaSubConfig
    {
        public string? GroupId { get; private set; }
        public string? BootstrapServers { get; private set; }
        public bool EnableAutoCommit { get; private set; } = false;
        public int? StatisticsIntervalMs { get; private set; } = 5000;
        public int? SessionTimeoutMs { get; private set; } = 6000;
        public bool? EnablePartitionEof { get; private set; } = true;
        public bool? ApiVersionRequest { get; private set; }
        public string? Topic { get; private set; }
        public string? TopicRetry { get; private set; }
        public string? TopicDeadLetter { get; private set; }
        public int MaxRetryAttempts { get; private set; } = 3;
        public AutoOffsetReset? AutoOffsetReset { get; private set; }

        public void SetGroupId(string groupId)
        {
            GroupId = groupId;
        }
        public void SetBootstrapServers(string bootstrapServers)
        {
            BootstrapServers = bootstrapServers;
        }
        public void SetEnableAutoCommit(bool enableAutoCommit)
        {
            EnableAutoCommit = enableAutoCommit;
        }
        public void SetStatisticsIntervalMs(int statisticsIntervalMs)
        {
            StatisticsIntervalMs = statisticsIntervalMs;
        }
        public void SetSessionTimeoutMs(int sessionTimeoutMs)
        {
            SessionTimeoutMs = sessionTimeoutMs;
        }
        public void SetEnablePartitionEof(bool enablePartitionEof)
        {
            EnablePartitionEof = enablePartitionEof;
        }
        public void SetApiVersionRequest(bool apiVersionRequest)
        {
            ApiVersionRequest = apiVersionRequest;
        }
        public void SetTopic(string topic)
        {
            Topic = topic;
            TopicRetry = $"{topic}-RETRY";
            TopicDeadLetter = $"{topic}-DLQ";
        }
        public void SetTopicRetry(string topicRetry)
        {
            TopicRetry = topicRetry;
        }
        public void SetTopicDeadLetter(string topicDeadLetter)
        {
            TopicDeadLetter = topicDeadLetter;
        }
        public void SetMaxRetryAttempts(int maxRetryAttempts)
        {
            MaxRetryAttempts = maxRetryAttempts;
        }
        public void SetAutoOffsetReset(AutoOffsetReset autoOffsetReset)
        {
            AutoOffsetReset = autoOffsetReset;
        }
    }
}
