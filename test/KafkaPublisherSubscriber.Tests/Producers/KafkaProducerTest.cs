using Confluent.Kafka;
using KafkaPublisherSubscriber.Producers;
using Moq;
using System.Text;
using KafkaPublisherSubscriber.Extensions;

namespace KafkaPublisherSubscriber.Tests.Producers
{
    public class KafkaProducerTests
    {
        [Fact]
        public async Task SendAsync_ValidMessage_ReturnsDeliveryResult()
        {
            // Arrange
            var producerSettingsMock = new Mock<KafkaProducerConfig>();
            var producerConfigAction = new Action<KafkaProducerConfigBuilder>(builder =>
            {
                producerSettingsMock.SetupGet(r => r.BootstrapServers).Returns("localhost:9092");
                producerSettingsMock.SetupGet(r => r.Topic).Returns("my-topic");
            });

            var kafkaProducerMock = new Mock<IKafkaProducer<Null, string>>();
            kafkaProducerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);

            var kafkaProducer = kafkaProducerMock.Object;

            // Mock the producer to return a simulated DeliveryResult
            var deliveryResult = new DeliveryResult<Null, string>
            {
                Topic = "my-topic",
                Partition = new Partition(0),
                Offset = new Offset(42),
                Message = new Message<Null, string> { Value = "Test Message" }
            };

            var topic = "my-topic";
            var message = "Test Message";
            kafkaProducerMock.Setup(producer => producer.SendAsync(topic, message, It.IsAny<Null>(), It.IsAny<Headers>()))
                             .ReturnsAsync(deliveryResult);

            // Act
            var result = await kafkaProducer.SendAsync(topic, message);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(topic, result.Topic);
            Assert.Equal(0, result.Partition.Value);
            Assert.Equal(42, result.Offset.Value);
            Assert.Equal(message, result.Message.Value);

            // Verify that SendAsync was called with the correct arguments
            kafkaProducerMock.Verify(producer => producer.SendAsync(topic, message, It.IsAny<Null>(), It.IsAny<Headers>()), Times.Once());
        }

        [Fact]
        public async Task SendAsync_ValidMessageWithHeaders_ReturnsDeliveryResult()
        {
            // Arrange
            var producerSettingsMock = new Mock<KafkaProducerConfig>();
            var producerConfigAction = new Action<KafkaProducerConfigBuilder>(builder =>
            {
                producerSettingsMock.SetupGet(r => r.BootstrapServers).Returns("localhost:9092");
                producerSettingsMock.SetupGet(r => r.Topic).Returns("my-topic");
            });

            var kafkaProducerMock = new Mock<IKafkaProducer<Null, string>>();
            kafkaProducerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);

            var kafkaProducer = kafkaProducerMock.Object;

            // Prepare headers
            var headers = new Headers
        {
            { "RetryCount", Encoding.UTF8.GetBytes(1.ToString()) }
        };

            // Mock the producer to return a simulated DeliveryResult
            var deliveryResult = new DeliveryResult<Null, string>
            {
                Topic = "my-topic",
                Partition = new Partition(0),
                Offset = new Offset(42),
                Message = new Message<Null, string> { Value = "Test Message with Headers", Headers = headers }
            };

            // Mock the SendAsync method to return the simulated DeliveryResult
            var topic = "my-topic";
            var message = "Test Message with Headers";
            kafkaProducerMock.Setup(producer => producer.SendAsync(topic, message, It.IsAny<Null>(), headers))
                             .ReturnsAsync(deliveryResult);

            // Act
            var result = await kafkaProducer.SendAsync(topic, message, headers: headers);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(topic, result.Topic);
            Assert.Equal(0, result.Partition.Value);
            Assert.Equal(42, result.Offset.Value);
            Assert.Equal(message, result.Message.Value);
            Assert.Equal(1, headers.GetRetryCountFromHeader());

            // Verify that SendAsync was called with the correct arguments
            kafkaProducerMock.Verify(producer => producer.SendAsync(topic, message, It.IsAny<Null>(), headers), Times.Once());
        }

        [Fact]
        public async Task SendBatchAsync_ValidMessages_MessagesSentToKafka()
        {
            // Arrange
            var producerSettingsMock = new Mock<KafkaProducerConfig>();
            var producerConfigAction = new Action<KafkaProducerConfigBuilder>(builder =>
            {
                producerSettingsMock.SetupGet(r => r.BootstrapServers).Returns("localhost:9092");
                producerSettingsMock.SetupGet(r => r.Topic).Returns("my-topic");
            });

            var kafkaProducerMock = new Mock<IKafkaProducer<Null, string>>();
            kafkaProducerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);

            var kafkaProducer = kafkaProducerMock.Object;

            // Prepare the list of messages to be sent
            var messages = new List<string>
        {
            "Message 1",
            "Message 2",
            "Message 3"
        };

            // Mock the producer to return simulated DeliveryResults
            var deliveryResults = new List<DeliveryResult<Null, string>>();
            for (int i = 0; i < messages.Count; i++)
            {
                var deliveryResult = new DeliveryResult<Null, string>
                {
                    Topic = "my-topic",
                    Partition = new Partition(0),
                    Offset = new Offset(42),
                    Message = new Message<Null, string> { Value = messages[i] }
                };

                deliveryResults.Add(deliveryResult);
            }

            var topic = "my-topic";

            // Mock the SendBatchAsync method to return the simulated DeliveryResults
            kafkaProducerMock.Setup(producer => producer.SendBatchAsync(topic, messages))
                             .Returns(Task.CompletedTask)
                             .Callback<string, IEnumerable<string>>((t, m) =>
                             {
                                 // Ensure that the provided topic and messages match the expected values
                                 Assert.Equal(topic, t);
                                 Assert.Equal(messages, m);
                             });

            // Act
            await kafkaProducer.SendBatchAsync(topic, messages);

            // Assert
            kafkaProducerMock.Verify(producer => producer.SendBatchAsync(topic, messages), Times.Once());
        }
    }
}
