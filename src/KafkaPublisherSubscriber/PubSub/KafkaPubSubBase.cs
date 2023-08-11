using Confluent.Kafka;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.Results;
using System.Text;
using System.Threading;

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
            var topics = GetSubscriptionTopics();
            Subscribe(topics);

 
            var tasks = new List<Task>();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = await ConsumeAsync(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        await HandleEndOfPartitionAync(tasks, consumeResult, cancellationToken);
                        continue;
                    }

                    await ProcessOrQueueMessageAsync(consumeResult, onMessageReceived, tasks, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

            }

            HandleConsumerCancellation();
        }
        private string[] GetSubscriptionTopics()
        {
            var topics = new List<string>
            {
                _kafkaFactory.SubConfig.Topic!
            };

            if (_kafkaFactory.SubConfig.EnableRetryTopicSubscription)
            {
                topics.Add(_kafkaFactory.SubConfig.TopicRetry!);
            }

            return topics.ToArray();
        }
        private async Task HandleEndOfPartitionAync(List<Task> tasks, ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken)
        {
            if (tasks.Any())
            {
                await ProcessTasksWithTimeoutAndClearAsync(tasks, cancellationToken);
            }

            Console.WriteLine($"Consumer has reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}");
            await Task.Delay(TimeSpan.FromSeconds(_kafkaFactory.SubConfig.DelayInSecondsPartitionEof), cancellationToken); // Insira um atraso conforme necessário
        }
        private async Task ProcessOrQueueMessageAsync(ConsumeResult<TKey, TValue> consumeResult, Func<ConsumeResult<TKey, TValue>, Task> onMessageReceived, List<Task> tasks, CancellationToken cancellationToken)
        {
            try
            {
                if (_kafkaFactory.SubConfig.ConsumerLimit == 0)
                {
                    _ = Task.Run(async () => await TryProcessMessageAsync(consumeResult: consumeResult,
                                                              onMessageReceived: onMessageReceived,
                                                              cancellationToken: cancellationToken), cancellationToken: cancellationToken);
                }
                else
                {
                    tasks.Add(TryProcessMessageAsync(consumeResult: consumeResult, onMessageReceived: onMessageReceived, cancellationToken: cancellationToken));

                    if (tasks.Count >= _kafkaFactory.SubConfig.ConsumerLimit)
                    {
                        var timeout = TimeSpan.FromSeconds(_kafkaFactory.SubConfig.TimeoutInSeconds);
                        await ProcessTasksWithTimeoutAndClearAsync(tasks, cancellationToken);
                    }
                }

                if (!_kafkaFactory.SubConfig.EnableAutoCommit)
                {
                    await CommitAsync(consumeResult, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                var retryCount = consumeResult.Message.Headers.GetHeaderAs<int>(Constants.HEADER_NAME_RETRY_COUNT);
                await HandleErrorAsync(consumeResult, ex, retryCount, cancellationToken);
            }
        }
        private async Task TryProcessMessageAsync(ConsumeResult<TKey, TValue> consumeResult, Func<ConsumeResult<TKey, TValue>, Task> onMessageReceived, CancellationToken cancellationToken)
        {
            try
            {
                await onMessageReceived(consumeResult);
            }
            catch (Exception ex)
            {
                var retryCount = consumeResult.Message.Headers.GetHeaderAs<int>(Constants.HEADER_NAME_RETRY_COUNT);
                await HandleErrorAsync(consumeResult, ex, retryCount, cancellationToken);
            }
        }
        private async Task HandleErrorAsync(ConsumeResult<TKey, TValue> consumeResult,
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
                    await PublishToRetryTopicAsync(consumeResult: consumeResult, retryCount: retryCount + 1, cancellationToken: cancellationToken);
                    // Retry the message by using the consumer and producer interfaces
                    await CommitAsync(consumeResult, cancellationToken); // Commit the offset before retrying
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
        public async Task ProcessTasksWithTimeoutAndClearAsync(List<Task> tasks, CancellationToken cancellationToken)
        {
            try
            {
                var timeout = TimeSpan.FromSeconds(_kafkaFactory.SubConfig.ProcessTimeoutInSeconds);

                await ExecuteTasksWithTimeoutsAsync(tasks, timeout, cancellationToken);
            }
            finally
            {
                ClearTasks(tasks);
            }
        }
        private static async Task ExecuteTasksWithTimeoutsAsync(List<Task> tasks, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var tasksWithTimeouts = tasks.Select(t => WithTimeoutAsync(t, timeout, cancellationToken)).ToList();
            await Task.WhenAll(tasksWithTimeouts);
        }
        private static async Task WithTimeoutAsync(Task task, TimeSpan timeout, CancellationToken externalCancellationToken = default)
        {
            if(timeout.TotalSeconds == 0)
            {
               await task;
               return;
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
            cts.CancelAfter(timeout);

            try
            {
                await task;
            }
            catch (TaskCanceledException) when (!externalCancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"Some tasks did not complete within {timeout.TotalSeconds} seconds and were timed out.");
                throw new TimeoutException();
            }
        }
        private static void ClearTasks(List<Task> tasks)
        {
            tasks.Clear();
        }
        private void HandleConsumerCancellation()
        {
            Console.WriteLine("Consumer Cancelled!");
            Dispose();
        }
        private async Task PublishToRetryTopicAsync(ConsumeResult<TKey, TValue> consumeResult, int retryCount, CancellationToken cancellationToken)
        {
            Headers existingHeaders = consumeResult.Message.Headers;
            existingHeaders.AddOrUpdate(Constants.HEADER_NAME_RETRY_COUNT, Encoding.UTF8.GetBytes(retryCount.ToString()));

            Message<TKey, TValue> kafkaMessage = _kafkaFactory.CreateKafkaMessage(message: consumeResult.Message.Value, key: consumeResult.Message.Key, headers: existingHeaders);

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


