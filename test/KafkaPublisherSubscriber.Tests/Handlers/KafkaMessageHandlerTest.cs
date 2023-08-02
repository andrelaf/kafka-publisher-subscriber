using Confluent.Kafka;
using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Handlers;
using KafkaPublisherSubscriber.Producers;
using Moq;
using System.Text;

namespace KafkaPublisherSubscriber.Tests.Handlers
{
    public class KafkaMessageHandlerTests
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

            var consumerSettingsMock = new Mock<KafkaConsumerConfig>();
            var consumerMock = new Mock<IKafkaConsumer<Ignore, string>>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(consumerSettingsMock.Object);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new Mock<KafkaProducerConfig>();
            var producerMock = new Mock<IKafkaProducer<Ignore, string>>();
            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);
            producerMock.Setup(producer => producer.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Ignore>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Ignore, string>());

            var handler = new KafkaMessageHandler<Ignore, string>(consumerMock.Object, producerMock.Object);

            // Act
            var subscribeTask = handler.Subscribe(async (msg) =>
            {
                // Process the message here (e.g., log it or handle it)
                await Task.Delay(100); // Simulate message processing time
            }, cancellationToken);

            await Task.Delay(500);
            cancellationTokenSource.Cancel();

            // Aguardar o término da execução do Subscribe
            await subscribeTask;

            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.AtLeastOnce());
            producerMock.Verify(producer => producer.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Ignore>(), It.IsAny<Headers>()), Times.Never());
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

            var consumerSettingsMock = new Mock<KafkaConsumerConfig>();
            var consumerMock = new Mock<IKafkaConsumer<Ignore, string>>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(consumerSettingsMock.Object);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new Mock<KafkaProducerConfig>();
            var producerMock = new Mock<IKafkaProducer<Ignore, string>>();
            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock.Object);
            producerMock.Setup(producer => producer.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Ignore>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Ignore, string>());

            var handler = new KafkaMessageHandler<Ignore, string>(consumerMock.Object, producerMock.Object);


            // Act
            cancellationTokenSource.CancelAfter(500);
            await handler.Subscribe((msg) =>
            {
                // Simulate an error during message processing
                throw new Exception("Simulated Error");
            }, cancellationToken);

      
            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.AtLeastOnce());
            producerMock.Verify(producer => producer.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Ignore>(), It.IsAny<Headers>()), Times.AtLeastOnce());
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
      
            var kafkaConsumerConfigMock = new KafkaConsumerConfigBuilder()
                .WithTopic("topic")
                .WithTopicRetry("topic-retry")
                .WithTopicDeadLetter("topic-dlq")
                .WithBootstrapServers("localhost:9092")
                .WithGroupId("group-test")
                .WithEnableAutoCommit(false)
                .WithEnablePartitionEof(true)
                .WithMaxRetryAttempts(3)
                .Build();


            var consumerMock = new Mock<IKafkaConsumer<Ignore, string>>();
            consumerMock.Setup(consumer => consumer.Settings).Returns(kafkaConsumerConfigMock);
            consumerMock.Setup(consumer => consumer.Consume(cancellationToken)).ReturnsAsync(consumeResult);

            var producerSettingsMock = new KafkaProducerConfig();
            var producerMock = new Mock<IKafkaProducer<Ignore, string>>();
            producerMock.Setup(producer => producer.Settings).Returns(producerSettingsMock);
            producerMock.Setup(producer => producer.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Ignore>(), It.IsAny<Headers>()))
                        .ReturnsAsync(new DeliveryResult<Ignore, string>());

            var handler = new KafkaMessageHandler<Ignore, string>(consumerMock.Object, producerMock.Object);

            // Act
            cancellationTokenSource.CancelAfter(500);
            await handler.Subscribe((msg) =>
            {

                throw new Exception("Simulated Error"); 
            }, cancellationToken);


            // Assert
            consumerMock.Verify(consumer => consumer.Consume(cancellationToken), Times.Once); // One initial try + 3 retries
            producerMock.Verify(producer => producer.SendAsync("topic-dlq", messageValue, It.IsAny<Ignore>(), It.IsAny<Headers>()), Times.Once());
        }
    }
}
