using Confluent.Kafka;
using KafkaPublisherSubscriber.Consumers;
using Moq;

namespace KafkaPublisherSubscriber.Tests.Consumers
{
    public class KafkaConsumerTests
    {
        [Fact]
        public async Task Consume_ValidMessage_MessageProcessed()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var message = new Message<Ignore, string>
            {
                Value = "Test Message"
            };
            var consumeResult = new ConsumeResult<Ignore, string>
            {
                Message = message
            };

            var consumerSettings = new KafkaConsumerSettingsBuilder()
                                    .WithBootstrapServers("localhost:9092")
                                    .WithTopic("my-topic")
                                    .WithGroupId("group-test")
                                    .WithEnableAutoCommit(false)
                                    .Build();

            // Setup the mock only for the Consume method
            var kafkaConsumerMock = new Mock<IKafkaConsumer>() { CallBase = true };
            kafkaConsumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            // Act
            var result = await kafkaConsumerMock.Object.Consume(cancellationToken);

            // Assert
            Assert.Equal(message.Value, result.Message.Value);
            kafkaConsumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.Once());
        }

        [Fact]
        public async Task Consume_ErrorProcessingMessage_HandleErrorCalled()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            // Simulate Kafka message
            var message = new Message<Ignore, string>
            {
                Value = "Test Message"
            };
            var consumeResultExpected = new ConsumeResult<Ignore, string>
            {
                Message = message
            };

            var consumerSettings = new KafkaConsumerSettingsBuilder()
                                    .WithBootstrapServers("localhost:9092")
                                    .WithTopic("my-topic")
                                    .WithGroupId("group-test")
                                    .WithEnableAutoCommit(false)
                                    .Build();

            var kafkaConsumer = new KafkaConsumer(consumerSettings);

            // Setup the mock only for the Consume method
            var kafkaConsumerMock = new Mock<IKafkaConsumer>() { CallBase = true };
            kafkaConsumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResultExpected);
            kafkaConsumerMock.Setup(consumer => consumer.Commit(consumeResultExpected)).Throws(new Exception("Simulated Error"));

            var consumeResult = await kafkaConsumerMock.Object.Consume(cancellationToken);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => kafkaConsumerMock.Object.Commit(consumeResult));

            // Assert
            Assert.Equal("Simulated Error", exception.Message);
            kafkaConsumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.Once());
            kafkaConsumerMock.Verify(consumer => consumer.Commit(consumeResult), Times.Once());
        }



    }
}
