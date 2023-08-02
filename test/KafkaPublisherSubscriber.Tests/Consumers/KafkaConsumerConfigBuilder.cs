
using Confluent.Kafka;
using KafkaPublisherSubscriber.Producers;
using Xunit;

namespace KafkaPublisherSubscriber.Tests.Consumers
{
    public class KafkaConsumerConfigBuilderTests
    {
        [Fact]
        public void Test_ValidConfiguration()
        {
            var builder = new KafkaConsumerConfigBuilder();
            var config = builder.WithGroupId("my_group")
                                .WithBootstrapServers("localhost:9092")
                                .WithEnableAutoCommit(true)
                                .WithStatisticsIntervalMs(5000)
                                .WithSessionTimeoutMs(60000)
                                .WithEnablePartitionEof(false)
                                .WithApiVersionRequest(true)
                                .WithTopic("my_topic")
                                .WithTopicRetry("retry_topic")
                                .WithTopicDeadLetter("dead_letter_topic")
                                .WithMaxRetryAttempts(3)
                                .WithAutoOffsetReset(AutoOffsetReset.Latest)
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("my_group", config.GroupId);
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.True(config.EnableAutoCommit);
            Assert.Equal(5000, config.StatisticsIntervalMs);
            Assert.Equal(60000, config.SessionTimeoutMs);
            Assert.False(config.EnablePartitionEof);
            Assert.True(config.ApiVersionRequest);
            Assert.Equal("my_topic", config.Topic);
            Assert.Equal("retry_topic", config.TopicRetry);
            Assert.Equal("dead_letter_topic", config.TopicDeadLetter);
            Assert.Equal(3, config.MaxRetryAttempts);
            Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
        }

        [Fact]
        public void Test_InvalidConfigurations()
        {
            var builder = new KafkaConsumerConfigBuilder();

            // Missing 'bootstrapServers'
            Assert.Throws<ArgumentNullException>(() => builder.WithGroupId("my_group").WithTopic("my_topic").Build());

            // Missing 'groupId'
            Assert.Throws<ArgumentNullException>(() => builder.WithBootstrapServers("localhost:9092").WithTopic("my_topic").Build());

            // Missing 'topic'
            Assert.Throws<ArgumentNullException>(() => builder.WithBootstrapServers("localhost:9092").WithGroupId("my_group").Build());
        }

        [Fact]
        public void Test_MinimumRequiredConfigurations()
        {
            var builder = new KafkaConsumerConfigBuilder();
            var config = builder.WithGroupId("my_group")
                                .WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("my_group", config.GroupId);
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.False(config.EnableAutoCommit); // Default value for bool type is false
            Assert.Equal(0, config.StatisticsIntervalMs); // Default value for int type is 0
            Assert.Equal(0, config.SessionTimeoutMs); // Default value for int type is 0
            Assert.False(config.EnablePartitionEof); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset); // Default value is AutoOffsetReset.Earliest
        }

        [Fact]
        public void Test_OptionalConfigurationsNotSet()
        {
            var builder = new KafkaConsumerConfigBuilder();
            var config = builder.WithGroupId("my_group")
                                .WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .WithMaxRetryAttempts(5)
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("my_group", config.GroupId);
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.False(config.EnableAutoCommit); // Default value for bool type is false
            Assert.Equal(0, config.StatisticsIntervalMs); // Default value for int type is 0
            Assert.Equal(0, config.SessionTimeoutMs); // Default value for int type is 0
            Assert.False(config.EnablePartitionEof); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal("my_topic", config.Topic);
            Assert.Null(config.TopicRetry); // Default value for string type is null
            Assert.Null(config.TopicDeadLetter); // Default value for string type is null
            Assert.Equal(5, config.MaxRetryAttempts);
            Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset); // Default value is AutoOffsetReset.Earliest
        }

        [Fact]
        public void Test_ChainingOfConfigurations()
        {
            var builder = new KafkaConsumerConfigBuilder();
            var config = builder.WithGroupId("my_group")
                                .WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .WithSessionTimeoutMs(30000)
                                .WithMaxRetryAttempts(2)
                                .WithBootstrapServers("new_server:9092") // Overwriting 'bootstrapServers'
                                .WithTopic("new_topic") // Overwriting 'topic'
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("my_group", config.GroupId);
            Assert.Equal("new_server:9092", config.BootstrapServers);
            Assert.False(config.EnableAutoCommit); // Default value for bool type is false
            Assert.Equal(0, config.StatisticsIntervalMs); // Default value for int type is 0
            Assert.Equal(30000, config.SessionTimeoutMs);
            Assert.False(config.EnablePartitionEof); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal("new_topic", config.Topic);
            Assert.Null(config.TopicRetry); // Default value for string type is null
            Assert.Null(config.TopicDeadLetter); // Default value for string type is null
            Assert.Equal(2, config.MaxRetryAttempts);
            Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset); // Default value is AutoOffsetReset.Earliest
        }

        [Fact]
        public void Test_DifferentAutoOffsetReset()
        {
            var builder = new KafkaConsumerConfigBuilder();
            var config1 = builder.WithGroupId("my_group")
                                 .WithBootstrapServers("localhost:9092")
                                 .WithTopic("my_topic")
                                 .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                                 .Build();

            var config2 = builder.WithGroupId("my_group")
                                 .WithBootstrapServers("localhost:9092")
                                 .WithTopic("my_topic")
                                 .WithAutoOffsetReset(AutoOffsetReset.Latest)
                                 .Build();

            // Assert expected values of the 'config' objects
            Assert.Equal(AutoOffsetReset.Earliest, config1.AutoOffsetReset);
            Assert.Equal(AutoOffsetReset.Latest, config2.AutoOffsetReset);
        }
    }
}
