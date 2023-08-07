using Confluent.Kafka;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.Results;
using System.Text;

namespace KafkaPublisherSubscriber.PubSub
{

    public abstract class KafkaPubSubBase<TKey, TValue> : IKafkaPubSub<TKey, TValue>
    {
        private bool disposedValue;

        private IProducer<TKey, TValue>? _producer;
        private IConsumer<TKey, TValue>? _consumer;
        private readonly IKafkaFactory _kafkaFactory;
        public KafkaPubSubBase(IKafkaFactory kafkaFactory)
        {
            _kafkaFactory = kafkaFactory;
        }

        public async Task<DeliveryResult<TKey, TValue>> SendAsync(TValue message, TKey key = default!, Headers headers = default!, CancellationToken cancellationToken = default!)
        {
            Message<TKey, TValue> kafkaMessage = _kafkaFactory.CreateKafkaMessage<TKey, TValue>(message, key, headers);

            return await SendAsync(topic: _kafkaFactory.PubConfig.Topic!, kafkaMessage: kafkaMessage, cancellationToken: cancellationToken);
        }
        public async Task<BatchSendResult<TKey, TValue>> SendBatchAsync(IEnumerable<Message<TKey, TValue>> kafkaMessages, CancellationToken cancellationToken = default!)
        {
            var result = new BatchSendResult<TKey, TValue>();

            foreach (var kafkaMessage in kafkaMessages)
            {
                try
                {
                    var deliveryResult = await SendAsync(topic: _kafkaFactory.PubConfig.Topic!, kafkaMessage: kafkaMessage, cancellationToken:  cancellationToken);
                    result.Successes.Add(deliveryResult);
                    Console.WriteLine($"Mensagem '{kafkaMessage}' enviada para partição: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");
                }
                catch (Exception e)
                {
                    result.Failures.Add((kafkaMessage, e));
                }
            }

            _producer!.Flush(TimeSpan.FromSeconds(10));

            return result;
        }
        private async Task<DeliveryResult<TKey, TValue>> SendAsync(string topic, Message<TKey, TValue> kafkaMessage, CancellationToken cancellationToken)
        {
            EnsureProducerConnection();

            return await _producer!.ProduceAsync(topic: topic, message: kafkaMessage, cancellationToken: cancellationToken);
        }


        public async Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken)
        {
            EnsurConsumerConnection();

            return await Task.Run(() => _consumer!.Consume(cancellationToken), cancellationToken: cancellationToken);
        }
        public async Task CommitAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancelationToken)
        {
            EnsurConsumerConnection();

            await Task.Run(() => _consumer!.Commit(consumeResult), cancellationToken: cancelationToken);
        }
        public void Subscribe(string[] topics)
        {
            EnsurConsumerConnection();

            _consumer!.Subscribe(topics);
        }
        
        private void EnsureProducerConnection()
        {
            _producer ??= _kafkaFactory.CreateProducer<TKey, TValue>();
        }
        private void EnsurConsumerConnection()
        {
            _consumer ??= _kafkaFactory.CreateConsumer<TKey, TValue>();
        }

        public async Task TryConsumeWithRetryFlowAsync(Func<ConsumeResult<TKey ,TValue>, Task> onMessageReceived, CancellationToken cancellationToken = default!)
        {
            var topics = new List<string>
            {
                _kafkaFactory.SubConfig.Topic!
            };

            if (!string.IsNullOrEmpty(_kafkaFactory.SubConfig.TopicRetry))
            {
                topics.Add(_kafkaFactory.SubConfig.TopicRetry);
            }

            Subscribe(topics.ToArray());

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = await ConsumeAsync(cancellationToken);

                    try
                    {
                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Consumer has reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}");
                            await Task.Delay(TimeSpan.FromSeconds(_kafkaFactory.SubConfig.DelayInSecondsPartitionEof), cancellationToken); // Insira um atraso conforme necessário
                            continue;
                        }


                        await onMessageReceived.Invoke(consumeResult);
                        if (!_kafkaFactory.SubConfig.EnableAutoCommit)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            await CommitAsync(consumeResult, cancellationToken); // Commit the offset since the message was successfully processed
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Consumer Cancelled!");
                        break; // Break the loop when the token is canceled
                    }
                    catch (Exception ex)
                    {
                        int retryCount = consumeResult.Message.Headers.GetHeaderAs<int>(Constants.HEADER_NAME_RETRY_COUNT);
                        await HandleError(consumeResult: consumeResult, exception: ex, retryCount: retryCount, cancellationToken: cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer Cancelled!");
            }
            finally
            {
                Dispose();
            }
        }
        private async Task HandleError(ConsumeResult<TKey, TValue> consumeResult,
                                                          Exception exception,
                                                          int retryCount,
                                                          CancellationToken cancellationToken)
        {
            // Log the error or perform any other error handling logic
            Console.WriteLine($"Error processing message: {consumeResult.Message.Value}. Exception: {exception}");

            // Retry the message if the retry count is less than the maximum allowed
            while (retryCount < _kafkaFactory.SubConfig.MaxRetryAttempts)
            {
                try
                {
                    // Retry the message by using the consumer and producer interfaces
                    await CommitAsync(consumeResult, cancellationToken); // Commit the offset before retrying
                    await PublishToRetryTopicAsync(message: consumeResult.Message.Value, key: consumeResult.Message.Key, retryCount: retryCount + 1, cancellationToken: cancellationToken);
                    break; // Exit the retry loop if the retry is successful
                }
                catch (Exception retryEx)
                {
                    // Log the retry error or perform any other retry error handling logic
                    Console.WriteLine($"Retry attempt {retryCount + 1} failed. Exception: {retryEx}");
                    retryCount++;
                }
            }

            if (retryCount >= _kafkaFactory.SubConfig.MaxRetryAttempts)
            {
                // The message has reached the maximum retry attempts, publish it to the dead letter queue
                await PublishToDeadLetterTopicAsync(kafkaMessage: consumeResult.Message, cancellationToken: cancellationToken);
            }

            if (!_kafkaFactory.SubConfig.EnableAutoCommit)
            {
                // Commit the offset since the message was processed (either retried or sent to the dead letter queue)
                await CommitAsync(consumeResult, cancellationToken);
            }
        }
        private async Task PublishToRetryTopicAsync(TValue message, TKey key, int retryCount = 0, CancellationToken cancellationToken = default!)
        {
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes(retryCount.ToString()) }
            };

            Message<TKey, TValue> kafkaMessage = _kafkaFactory.CreateKafkaMessage(message, key, headers);

            _ = await SendAsync(topic: _kafkaFactory.SubConfig.TopicRetry!, kafkaMessage: kafkaMessage, cancellationToken: cancellationToken);
        }
        private async Task PublishToDeadLetterTopicAsync(Message<TKey, TValue> kafkaMessage, CancellationToken cancellationToken)
        {
            try
            {
               _ = await SendAsync(topic: _kafkaFactory.SubConfig.TopicDeadLetter!, kafkaMessage: kafkaMessage, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while publishing message to dlq exception: {ex}.");
            }          
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _consumer?.Close() ;
                    _consumer?.Dispose() ;
                    _producer?.Dispose() ;
                }

                _consumer = null;
                _producer = null;
    

                disposedValue = true;
            }
        }
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

}


