using Castle.Components.DictionaryAdapter;
using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.PubSub;
using KafkaPublisherSubscriber.Tests.Mocks;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPublisherSubscriber.Tests
{
    public class ServicesCollectionExtensionTest
    {
        private readonly IServiceCollection _services;
        private Action<KafkaSubConfig>? _kafkaSubConfigAction;
        private Action<KafkaPubConfig>? _kafkaPubConfigAction;

        public ServicesCollectionExtensionTest()
        {
            _services = new ServiceCollection();
        }

        [Fact]
        public void AddKafkaConsumer_OnlyPubConfig_DoesNotThrowException()
        {
            // Arrange
            _kafkaPubConfigAction = config =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetTopic("test-topic");
            };

            // Act
            var serviceCollection = _services.AddKafkaPubSub<IPubSubImplementationMock, PubSubImplementationMock>(_kafkaSubConfigAction!, _kafkaPubConfigAction);

            // Assert
            // Assert
            Assert.NotNull(serviceCollection);
            Assert.IsAssignableFrom<IServiceCollection>(serviceCollection);
            Assert.Equal(2, serviceCollection.Count);
            Assert.Contains(serviceCollection, service => service.ServiceType == typeof(IPubSubImplementationMock));
            bool containsKafkaFactory = serviceCollection.Any(serviceDescriptor => serviceDescriptor.ServiceType == typeof(IKafkaFactory));
            Assert.True(containsKafkaFactory, "Expected IKafkaFactory to be registered in the service collection, but it was not.");
        }

        [Fact]
        public void AddKafkaConsumer_BothConfigNull_ThrowsException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => _services.AddKafkaPubSub<IPubSubImplementationMock, PubSubImplementationMock>());
        }
    }
}