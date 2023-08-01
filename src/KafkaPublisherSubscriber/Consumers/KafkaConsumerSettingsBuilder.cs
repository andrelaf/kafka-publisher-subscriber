using Confluent.Kafka;
using System;

namespace KafkaPublisherSubscriber.Consumers
{
    public class KafkaConsumerSettingsBuilder
    {
        private readonly KafkaConsumerSettings settings = new KafkaConsumerSettings();

        public KafkaConsumerSettingsBuilder WithGroupId(string groupId)
        {
            settings.SetGroupId(groupId);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithBootstrapServers(string bootstrapServers)
        {
            settings.SetBootstrapServers(bootstrapServers);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithEnableAutoCommit(bool enableAutoCommit)
        {
            settings.SetEnableAutoCommit(enableAutoCommit);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithStatisticsIntervalMs(int statisticsIntervalMs)
        {
            settings.SetStatisticsIntervalMs(statisticsIntervalMs);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithSessionTimeoutMs(int sessionTimeoutMs)
        {
            settings.SetSessionTimeoutMs(sessionTimeoutMs);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithEnablePartitionEof(bool enablePartitionEof)
        {
            settings.SetEnablePartitionEof(enablePartitionEof);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithApiVersionRequest(bool apiVersionRequest)
        {
            settings.SetApiVersionRequest(apiVersionRequest);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithTopic(string topic)
        {
            settings.SetTopic(topic);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithRetryTopic(string retryTopic)
        {
            settings.SetTopicRetry(retryTopic);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithDeadLetterTopic(string deadLetterTopic)
        {
            settings.SetTopicDeadLetter(deadLetterTopic);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithMaxRetryAttempts(int maxRetryAttempts)
        {
            settings.SetMaxRetryAttempts(maxRetryAttempts);
            return this;
        }

        public KafkaConsumerSettingsBuilder WithAutoOffsetReset(AutoOffsetReset autoOffsetReset)
        {
            settings.SetAutoOffsetReset(autoOffsetReset);
            return this;
        }

        public KafkaConsumerSettings Build()
        {
            if (settings.BootstrapServers is null || settings.GroupId is null || settings.Topic is null)
            {
                throw new ArgumentNullException($"Arguments required {nameof(settings.BootstrapServers)}, {nameof(settings.GroupId)}, {nameof(settings.Topic)}.");
            }

            return settings;
        }
    }
}
