using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Handlers;
using KafkaPublisherSubscriber.Producers;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPublisherSubscriber.Tests
{
    public class DependencyInjectionConfigTests
    {
        [Fact]
        public void AddKafkaProducer_ValidConfig_ReturnsCorrectServiceCollection()
        {
            // Arrange
            var services = new ServiceCollection();
            var producerConfigAction = new Action<KafkaProducerSettingsBuilder>(builder =>
            {
                builder.WithBootstrapServers("localhost:9092");
                builder.WithTopic("test-topic");
            });

            // Act
            var result = DependencyInjectionConfig.AddKafkaProducer(services, producerConfigAction);

            // Assert
            Assert.NotNull(result);
            Assert.IsAssignableFrom<IServiceCollection>(result);
            Assert.Contains(result, service => service.ServiceType == typeof(IKafkaProducer));
            Assert.Single(result);
        }

        [Fact]
        public void AddKafkaConsumer_ValidConfig_ReturnsCorrectServiceCollection()
        {
            // Arrange
            var services = new ServiceCollection();
            var consumerConfigAction = new Action<KafkaConsumerSettingsBuilder>(builder =>
            {
                builder.WithBootstrapServers("localhost:9092");
                builder.WithTopic("test-topic");
                builder.WithGroupId("group-test");
            });

            // Act
            var result = DependencyInjectionConfig.AddKafkaConsumer(services, consumerConfigAction);

            // Assert
            Assert.NotNull(result);
            Assert.IsAssignableFrom<IServiceCollection>(result);
            Assert.Contains(result, service => service.ServiceType == typeof(IKafkaConsumer));
            Assert.Single(result);
        }

        [Fact]
        public void AddKafkaProducerAndConsumer_ValidConfig_ReturnsCorrectServiceCollection()
        {
            // Arrange
            var services = new ServiceCollection();
            var producerConfigAction = new Action<KafkaProducerSettingsBuilder>(builder =>
            {
                builder.WithBootstrapServers("localhost:9092");
                builder.WithTopic("test-topic");
            });
            var consumerConfigAction = new Action<KafkaConsumerSettingsBuilder>(builder =>
            {
                builder.WithBootstrapServers("localhost:9092");
                builder.WithTopic("test-topic");
                builder.WithGroupId("group-test");
            });

            // Act
            var result = DependencyInjectionConfig.AddKafkaProducerAndConsumer(services, consumerConfigAction, producerConfigAction);

            // Assert
            Assert.NotNull(result);
            Assert.IsAssignableFrom<IServiceCollection>(result);
            Assert.Contains(result, service => service.ServiceType == typeof(IKafkaConsumer));
            Assert.Contains(result, service => service.ServiceType == typeof(IKafkaProducer));
            Assert.Contains(result, service => service.ServiceType == typeof(IKafkaMessageHandler));
            Assert.Equal(3, result.Count);
        }
    }

}