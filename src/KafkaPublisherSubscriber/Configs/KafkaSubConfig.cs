﻿using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Configs
{
    [ExcludeFromCodeCoverage]
    public sealed class KafkaSubConfig
    {
        public string? GroupId { get; private set; }
        public string? BootstrapServers { get; private set; }
        public bool EnableAutoCommit { get; private set; } = false;
        public int? StatisticsIntervalMs { get; private set; } = 10000;
        public int? SessionTimeoutMs { get; private set; } = 10000;
        public bool EnablePartitionEof { get; private set; } = false;
        public bool EnableRetryTopicSubscription { get; private set; } = false;
        public bool? ApiVersionRequest { get; private set; }
        public string? Topic { get; private set; }
        public string? TopicRetry { get; private set; }
        public string? TopicDeadLetter { get; private set; }
        public AutoOffsetReset AutoOffsetReset { get; private set; } = AutoOffsetReset.Latest;
        public int MaxRetryAttempts { get; private set; } = 3;
        public int DelayInSecondsPartitionEof { get; private set; } = 1;
        public int? ConsumerLimit { get; private set; }

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
        public void SetEnableRetryTopicSubscription(bool enableRetryTopicSubscription)
        {
            EnableRetryTopicSubscription = enableRetryTopicSubscription;
        }
        public void SetApiVersionRequest(bool apiVersionRequest)
        {
            ApiVersionRequest = apiVersionRequest;
        }
        public void SetTopic(string topic)
        {
            Topic = topic;
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
        public void SetMaxRetryAttempts(int maxRetryAttempts)
        {
            MaxRetryAttempts = maxRetryAttempts;
        }
        public void SetDelayInSecondsPartitionEof(int delayInSecondsPartitionEof)
        {
            DelayInSecondsPartitionEof = delayInSecondsPartitionEof;
        }  
        public void SetConsumerLimit(int consumerLimit)
        {
            ConsumerLimit = consumerLimit;
        }
    }
}
