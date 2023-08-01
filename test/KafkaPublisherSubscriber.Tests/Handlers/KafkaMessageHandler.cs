using Confluent.Kafka;
using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Producers;
using Moq;
using System.Text;

namespace KafkaPublisherSubscriber.Tests.Handlers
{
    public class KafkaMessageHandler
    {
        [Fact]
        public async Task Subscribe_ValidMessage_MessageProcessed()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var messageValue = "Test Message";
            var message = new Message<Ignore, string>
            {
                Value = messageValue
            };

            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = message
            };

            var consumerSettingsMock = new Mock<KafkaConsumerSettings>();
            var consumerMock = new Mock<IKafkaConsumer>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(consumerSettingsMock.Object);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new Mock<KafkaProducerSettings>();
            var producerMock = new Mock<IKafkaProducer>();

            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);
            producerMock.Setup(producer => producer.SendMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Null, string>());

            var handler = new KafkaPublisherSubscriber.Handlers.KafkaMessageHandler(consumerMock.Object, producerMock.Object);

            // Act
            await handler.Subscribe(async (msg) =>
            {
                // Process the message here (e.g., log it or handle it)
                await Task.Delay(100); // Simulate message processing time
            }, cancellationToken);

            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.AtLeastOnce());
            producerMock.Verify(producer => producer.SendMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Headers>()), Times.Never());
        }

        [Fact]
        public async Task Subscribe_ErrorProcessingMessage_RetriesAndSendsToDeadLetterQueue()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var messageValue = "Test Message";
            var message = new Message<Ignore, string>
            {
                Value = messageValue
            };
            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = message
            };

            var consumerSettingsMock = new Mock<KafkaConsumerSettings>();
            var consumerMock = new Mock<IKafkaConsumer>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(consumerSettingsMock.Object);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new Mock<KafkaProducerSettings>();
            var producerMock = new Mock<IKafkaProducer>();
            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);
            producerMock.Setup(producer => producer.SendMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Null, string>());

            var handler = new KafkaPublisherSubscriber.Handlers.KafkaMessageHandler(consumerMock.Object, producerMock.Object);

            // Act
            await handler.Subscribe(async (msg) =>
            {
                // Simulate an error during message processing
                throw new Exception("Simulated Error");
            }, cancellationToken);

            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.AtLeastOnce());
            producerMock.Verify(producer => producer.SendMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Headers>()), Times.Exactly(consumerSettingsMock.Object.MaxRetryAttempts));
        }

        [Fact]
        public async Task Subscribe_ErrorProcessingMessage_MaxRetryAttemptsReached_SendsToDeadLetterQueue()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes("3") }
            };

            var messageValue = "Test Message";
            var message = new Message<Ignore, string>
            {
                Value = messageValue,
                Headers = headers
            };

            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = message,
            };

            var consumerSettingsMock = new Mock<KafkaConsumerSettings>();
            consumerSettingsMock.SetupGet(settings => settings.TopicRetry).Returns("retry-topic");
            consumerSettingsMock.SetupGet(settings => settings.TopicDeadLetter).Returns("dead-letter-topic");
            consumerSettingsMock.SetupGet(settings => settings.MaxRetryAttempts).Returns(3);

            var consumerMock = new Mock<IKafkaConsumer>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(consumerSettingsMock.Object);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new Mock<KafkaProducerSettings>();
            var producerMock = new Mock<IKafkaProducer>();
            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);
            producerMock.Setup(producer => producer.SendMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Null, string>());

            var handler = new KafkaPublisherSubscriber.Handlers.KafkaMessageHandler(consumerMock.Object, producerMock.Object);

            // Act
            await handler.Subscribe(async (msg) =>
            {
                // Simulate an error during message processing
                throw new Exception("Simulated Error");
            }, cancellationToken);

            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.Exactly(4)); // One initial try + 3 retries
            producerMock.Verify(producer => producer.SendMessageAsync("dead-letter-topic", messageValue), Times.Once());
        }
    }
}
