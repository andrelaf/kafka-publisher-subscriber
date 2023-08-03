﻿using Confluent.Kafka;
using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.PubSub;
using KafkaPublisherSubscriber.Tests.Mocks;
using Moq;

namespace KafkaPublisherSubscriber.Tests.PubSub
{
    public class KafkaPubSubTests
    {
        private readonly Mock<IKafkaFactory> _kafkaFactoryMock;
        private readonly Mock<IProducer<string, string>> _producerMock;
        private readonly Mock<IConsumer<string, string>> _consumerMock;
        private readonly KafkaPubSub<string, string> _kafkaPubSub;

        public KafkaPubSubTests()
        {
            _kafkaFactoryMock = new Mock<IKafkaFactory>();
            _producerMock = new Mock<IProducer<string, string>>();
            _consumerMock = new Mock<IConsumer<string, string>>();

            _kafkaFactoryMock.Setup(f => f.CreateProducer<string, string>()).Returns(_producerMock.Object);
            _kafkaFactoryMock.Setup(f => f.CreateConsumer<string, string>()).Returns(_consumerMock.Object);

            _kafkaPubSub = new KafkaPubSub<string, string>(_kafkaFactoryMock.Object);
        }

        [Fact]
        public async Task SendAsync_ValidInput_CallsProducerProduceAsync()
        {
            // Arrange
            var key = "key";
            var value = "value";
            var headers = new Headers();

            var kafkaMessage = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = headers
            };

            KafkaPubConfig pubConfig = new();
            ((Action<KafkaPubConfig>)((config) =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetAcks(Acks.All);
                // Adicione mais configurações conforme necessário
            }))(pubConfig);

            _kafkaFactoryMock.Setup(x => x.PubConfig).Returns(pubConfig);

            var deliveryResultMock = new Mock<DeliveryResult<string, string>>();
            _producerMock.Setup(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.Is<Message<string, string>>(m => m.Key == key && m.Value == value),
                It.IsAny<CancellationToken>()
                )).ReturnsAsync(deliveryResultMock.Object);

            // Act
            var result = await _kafkaPubSub.SendAsync(value, key, headers);

            // Assert
            _producerMock.Verify(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.Is<Message<string, string>>(m => m.Key == key && m.Value == value),
                It.IsAny<CancellationToken>()
                ), Times.Once);

            Assert.Same(deliveryResultMock.Object, result);
        }

        [Fact]
        public async Task ConsumeAsync_ValidInput_CallsConsumerConsume()
        {
            // Arrange
            var cancellationToken = new CancellationToken();
            _consumerMock
                .Setup(c => c.Consume(cancellationToken))
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _kafkaPubSub.ConsumeAsync(cancellationToken);

            // Assert
            _consumerMock.Verify(c => c.Consume(cancellationToken), Times.Once);
        }

        [Fact]
        public async Task SendBatchAsync_ValidInput_CallsProducerProduceAsyncForEachMessage()
        {
            // Arrange
            var messages = new List<string>
            {
                "Mensage 1",
                "Message 2",
                "Message 3"
            };

            var deliveryResultMock = new Mock<DeliveryResult<string, string>>();
            _producerMock.Setup(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.Is<Message<string, string>>(m => messages.Contains(m.Value)),
                It.IsAny<CancellationToken>()
                )).ReturnsAsync(deliveryResultMock.Object);

            KafkaPubConfig pubConfig = new();
            ((Action<KafkaPubConfig>)((config) =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetAcks(Acks.All);
                // Adicione mais configurações conforme necessário
            }))(pubConfig);

            _kafkaFactoryMock.Setup(x => x.PubConfig).Returns(pubConfig);


            // Act
            await _kafkaPubSub.SendBatchAsync(messages);

            // Assert
            _producerMock.Verify(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.Is<Message<string, string>>(m => messages.Contains(m.Value)),
                It.IsAny<CancellationToken>()
                ), Times.Exactly(messages.Count));
        }

        [Fact]
        public async Task CommitAsync_ValidInput_CallsConsumerCommit()
        {
            // Arrange
            var consumeResult = new ConsumeResult<string, string>();

            // Act
            await _kafkaPubSub.CommitAsync(consumeResult);

            // Assert
            _consumerMock.Verify(c => c.Commit(consumeResult), Times.Once);
        }

        [Fact]
        public void Subscribe_ValidInput_CallsConsumerSubscribe()
        {
            // Arrange
            var topics = new string[] { "topic1", "topic2" };

            // Act
            _kafkaPubSub.Subscribe(topics);

            // Assert
            _consumerMock.Verify(c => c.Subscribe(It.IsAny<IEnumerable<string>>()), Times.Once);
        }

        [Fact]
        public async Task TryConsumeWithRetryFlowAsync_ConsumesAndProcessesMessage()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var message = new Message<string, string>
            {
                Key = "key",
                Value = "value",
                Headers = new Headers(),
                Timestamp = new Timestamp(DateTime.UtcNow)
            };

            var consumeResult = new ConsumeResult<string, string>
            {
                TopicPartitionOffset = new TopicPartitionOffset("test-topic", new Partition(1), new Offset(1)),
                Message = message
            };

            _consumerMock.SetupSequence(x => x.Consume(cancellationToken))
                .Returns(consumeResult)
                .Throws(new OperationCanceledException()); // to stop the infinite loop


            KafkaSubConfig subConfig = new();
            ((Action<KafkaSubConfig>)((config) =>
            {
                config.SetTopic("TestTopic");
                config.SetTopicRetry("TestTopicRetry");
                config.SetMaxRetryAttempts(3);
                config.SetEnableAutoCommit(false);
            }))(subConfig);

            _kafkaFactoryMock.Setup(x => x.SubConfig).Returns(subConfig);

            // Act
            await _kafkaPubSub.TryConsumeWithRetryFlowAsync(_ => Task.CompletedTask, cancellationToken);

            // Assert
            _consumerMock.Verify(x => x.Subscribe(It.IsAny<string[]>()), Times.Once);
            _consumerMock.Verify(x => x.Consume(cancellationToken), Times.Exactly(2));
            _consumerMock.Verify(x => x.Commit(consumeResult), Times.Once);
        }

        [Fact]
        public async Task TryConsumeWithRetryFlowAsync_ConsumesAndProcessesMessage_PublishToRetry()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var message = new Message<string, string>
            {
                Key = "key",
                Value = "value",
                Headers = new Headers(),
                Timestamp = new Timestamp(DateTime.UtcNow)
            };

            var consumeResult = new ConsumeResult<string, string>
            {
                TopicPartitionOffset = new TopicPartitionOffset("test-topic", new Partition(1), new Offset(1)),
                Message = message
            };

            var messageProcessingException = new Exception("Test exception");

            var counter = 0;
            _consumerMock.Setup(x => x.Consume(cancellationToken))
                .Returns(() =>
                {
                    counter++;
                    if (counter == 3)
                    {
                        throw new OperationCanceledException(); // to stop the infinite loop
                    }
                    else
                    {
                        return consumeResult; // first and second calls to Consume
                    }
                });

            _consumerMock.Setup(x => x.Commit(consumeResult));

            KafkaSubConfig subConfig = new();
            ((Action<KafkaSubConfig>)((config) =>
            {
                config.SetTopic("TestTopic");
                config.SetTopicRetry("TestTopicRetry");
                config.SetMaxRetryAttempts(3);
                config.SetEnableAutoCommit(false);
            }))(subConfig);

            _kafkaFactoryMock.Setup(x => x.SubConfig).Returns(subConfig);

            KafkaPubConfig pubConfig = new();
            ((Action<KafkaPubConfig>)((config) =>
            {
                config.SetBootstrapServers("localhost:9092");
                config.SetAcks(Acks.All);
                // Add more configurations as needed
            }))(pubConfig);

            _kafkaFactoryMock.Setup(x => x.PubConfig).Returns(pubConfig);

            _producerMock.Setup(x => x.ProduceAsync(It.Is<string>(s => s == "TestTopicRetry"),
                                                    It.IsAny<Message<string, string>>(),
                                                    It.IsAny<CancellationToken>()))
                         .Returns((string topic, Message<string, string> message, CancellationToken cancellationToken) =>
                                   Task.FromResult(new DeliveryResult<string, string>()));

            // Act
            await _kafkaPubSub.TryConsumeWithRetryFlowAsync(message => message.Message.Value == "value" ? Task.FromException(messageProcessingException) : Task.CompletedTask, cancellationToken);

            // Assert
            _consumerMock.Verify(x => x.Subscribe(It.IsAny<string[]>()), Times.Once);
            _consumerMock.Verify(x => x.Consume(cancellationToken), Times.Exactly(3));
            _consumerMock.Verify(x => x.Commit(consumeResult), Times.Exactly(4)); // Changed from 4 to 2 here, assuming Commit is called every time Consume is called.
            _producerMock.Verify(x => x.ProduceAsync(It.Is<string>(s => s == "TestTopicRetry"),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()), Times.Exactly(2));
        }
        

    }
}
