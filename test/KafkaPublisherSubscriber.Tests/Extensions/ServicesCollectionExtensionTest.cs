using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.Tests.Mocks;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPublisherSubscriber.Tests
{
    public class ServicesCollectionExtensionTest
    {
        private readonly IServiceCollection _services;
        private Action<KafkaSubConfig>? _subConfigAction;
        private Action<KafkaPubConfig>? _pubConfigAction;

        public ServicesCollectionExtensionTest()
        {
            _services = new ServiceCollection();
        }

        [Fact]
        public void AddKafkaConsumer_OnlyPubConfig_DoesNotThrowException()
        {
            // Arrange
            _pubConfigAction = config =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetTopic("test-topic");
            };

            // Act
            var serviceCollection = _services.AddKafkaPubSub<IPubSubImplementationMock, PubSubImplementationMock>(_subConfigAction!, _pubConfigAction);

            // Assert
            // Assert
            Assert.NotNull(serviceCollection);
            Assert.IsAssignableFrom<IServiceCollection>(serviceCollection);
            Assert.Equal(1, serviceCollection.Count);
            Assert.Contains(serviceCollection, service => service.ServiceType == typeof(IPubSubImplementationMock));
        }


        [Fact]
        public void AddKafkaConsumer_OnlySubConfig_DoesNotThrowException()
        {
            // Arrange
            _subConfigAction = config =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetTopic("test-topic");
                config.SetGroupId("group-test");
                config.SetConsumerLimit(5);
            };

            // Act
            var serviceCollection = _services.AddKafkaPubSub<IPubSubImplementationMock, PubSubImplementationMock>(_subConfigAction, _pubConfigAction!);

            // Assert
            Assert.NotNull(serviceCollection);
            Assert.IsAssignableFrom<IServiceCollection>(serviceCollection);
            Assert.Equal(1, serviceCollection.Count);
            Assert.Contains(serviceCollection, service => service.ServiceType == typeof(IPubSubImplementationMock));
        }

        [Fact]
        public void AddKafkaConsumer_BothConfigNull_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _services.AddKafkaPubSub<IPubSubImplementationMock, PubSubImplementationMock>());
        }
    }
}