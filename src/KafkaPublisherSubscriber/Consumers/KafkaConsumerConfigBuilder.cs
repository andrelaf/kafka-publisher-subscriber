using Confluent.Kafka;
using System;

namespace KafkaPublisherSubscriber.Consumers
{
    public class KafkaConsumerConfigBuilder
    {
        private readonly KafkaConsumerConfig settings = new KafkaConsumerConfig();

        public KafkaConsumerConfigBuilder WithGroupId(string groupId)
        {
            settings.SetGroupId(groupId);
            return this;
        }

        public KafkaConsumerConfigBuilder WithBootstrapServers(string bootstrapServers)
        {
            settings.SetBootstrapServers(bootstrapServers);
            return this;
        }

        public KafkaConsumerConfigBuilder WithEnableAutoCommit(bool enableAutoCommit)
        {
            settings.SetEnableAutoCommit(enableAutoCommit);
            return this;
        }

        public KafkaConsumerConfigBuilder WithStatisticsIntervalMs(int statisticsIntervalMs)
        {
            settings.SetStatisticsIntervalMs(statisticsIntervalMs);
            return this;
        }

        public KafkaConsumerConfigBuilder WithSessionTimeoutMs(int sessionTimeoutMs)
        {
            settings.SetSessionTimeoutMs(sessionTimeoutMs);
            return this;
        }

        public KafkaConsumerConfigBuilder WithEnablePartitionEof(bool enablePartitionEof)
        {
            settings.SetEnablePartitionEof(enablePartitionEof);
            return this;
        }

        public KafkaConsumerConfigBuilder WithApiVersionRequest(bool apiVersionRequest)
        {
            settings.SetApiVersionRequest(apiVersionRequest);
            return this;
        }

        public KafkaConsumerConfigBuilder WithTopic(string topic)
        {
            settings.SetTopic(topic);
            return this;
        }

        public KafkaConsumerConfigBuilder WithTopicRetry(string retryTopic)
        {
            settings.SetTopicRetry(retryTopic);
            return this;
        }

        public KafkaConsumerConfigBuilder WithTopicDeadLetter(string deadLetterTopic)
        {
            settings.SetTopicDeadLetter(deadLetterTopic);
            return this;
        }

        public KafkaConsumerConfigBuilder WithMaxRetryAttempts(int maxRetryAttempts)
        {
            settings.SetMaxRetryAttempts(maxRetryAttempts);
            return this;
        }

        public KafkaConsumerConfigBuilder WithAutoOffsetReset(AutoOffsetReset autoOffsetReset)
        {
            settings.SetAutoOffsetReset(autoOffsetReset);
            return this;
        }

        public KafkaConsumerConfig Build()
        {
            if (settings.BootstrapServers is null || settings.GroupId is null || settings.Topic is null)
            {
                throw new ArgumentNullException($"Arguments required {nameof(settings.BootstrapServers)}, {nameof(settings.GroupId)}, {nameof(settings.Topic)}.");
            }

            return settings;
        }
    }
}
