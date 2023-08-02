namespace KafkaPublisherSubscriber.Tests.Consumers
{
    using Confluent.Kafka;
    using KafkaPublisherSubscriber.Producers;
    using Xunit;

    public class KafkaProducerConfigBuilderTests
    {
        [Fact]
        public void Test_ValidConfiguration()
        {
            var builder = new KafkaProducerConfigBuilder();
            var config = builder.WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .WithEnableIdempotence(true)
                                .WithApiVersionRequest(true)
                                .WithMessageSendMaxRetries(3)
                                .WithAcks(Acks.All)
                                .WithMaxInFlight(100)
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("my_topic", config.Topic);
            Assert.True(config.EnableIdempotence);
            Assert.True(config.ApiVersionRequest);
            Assert.Equal(3, config.MessageSendMaxRetries);
            Assert.Equal(Acks.All, config.Acks);
            Assert.Equal(100, config.MaxInFlight);
        }

        [Fact]
        public void Test_MinimumRequiredConfigurations()
        {
            var builder = new KafkaProducerConfigBuilder();
            var config = builder.WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("my_topic", config.Topic);
            Assert.False(config.EnableIdempotence); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal(0, config.MessageSendMaxRetries); // Default value for int type is 0
            Assert.Equal(Acks.All, config.Acks); // Default value is Acks.All
            Assert.Equal(0, config.MaxInFlight); // Default value for int type is 0
        }

        [Fact]
        public void Test_InvalidConfigurations()
        {
            var builder = new KafkaProducerConfigBuilder();

            // Missing 'bootstrapServers'
            Assert.Throws<ArgumentNullException>(() => builder.WithTopic("my_topic").Build());

            // Missing 'topic'
            Assert.Throws<ArgumentNullException>(() => builder.WithBootstrapServers("localhost:9092").Build());
        }

        [Fact]
        public void Test_OptionalConfigurationsNotSet()
        {
            var builder = new KafkaProducerConfigBuilder();
            var config = builder.WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .WithAcks(Acks.Leader)
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("my_topic", config.Topic);
            Assert.False(config.EnableIdempotence); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal(0, config.MessageSendMaxRetries); // Default value for int type is 0
            Assert.Equal(Acks.Leader, config.Acks);
            Assert.Equal(0, config.MaxInFlight); // Default value for int type is 0
        }

        [Fact]
        public void Test_ChainingOfConfigurations()
        {
            var builder = new KafkaProducerConfigBuilder();
            var config = builder.WithBootstrapServers("localhost:9092")
                                .WithTopic("my_topic")
                                .WithMessageSendMaxRetries(5)
                                .WithAcks(Acks.All)
                                .WithBootstrapServers("new_server:9092") // Overwriting 'bootstrapServers'
                                .Build();

            // Assert expected values of the 'config' object
            Assert.Equal("new_server:9092", config.BootstrapServers);
            Assert.Equal("my_topic", config.Topic);
            Assert.False(config.EnableIdempotence); // Default value for bool type is false
            Assert.False(config.ApiVersionRequest); // Default value for bool type is false
            Assert.Equal(5, config.MessageSendMaxRetries);
            Assert.Equal(Acks.All, config.Acks);
            Assert.Equal(0, config.MaxInFlight); // Default value for int type is 0
        }

        [Fact]
        public void Test_DifferentCombinationsOfAcks()
        {
            var builder = new KafkaProducerConfigBuilder();
            var config1 = builder.WithBootstrapServers("localhost:9092")
                                 .WithTopic("my_topic")
                                 .WithAcks(Acks.All)
                                 .Build();

            var config2 = builder.WithBootstrapServers("localhost:9092")
                                 .WithTopic("my_topic")
                                 .WithAcks(Acks.Leader)
                                 .Build();

            var config3 = builder.WithBootstrapServers("localhost:9092")
                                 .WithTopic("my_topic")
                                 .WithAcks(Acks.None)
                                 .Build();

            // Assert expected values of the 'config' objects
            Assert.Equal(Acks.All, config1.Acks);
            Assert.Equal(Acks.Leader, config2.Acks);
            Assert.Equal(Acks.None, config3.Acks);
        }
    }
}
