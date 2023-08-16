using Confluent.Kafka;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.Results;
using System.Text;

namespace KafkaPublisherSubscriber.PubSub;

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
                var deliveryResult = await SendAsync(topic: _kafkaFactory.PubConfig.Topic!, kafkaMessage: kafkaMessage, cancellationToken: cancellationToken);
                result.Successes.Add(deliveryResult);
                Console.WriteLine($"Mensagem '{kafkaMessage}' enviada para partição: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");
            }
            catch (Exception e)
            {
                result.Failures.Add((kafkaMessage, e));
            }
        }

        _producer!.Flush(timeout: TimeSpan.FromSeconds(10));

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

        Func<Task<ConsumeResult<TKey, TValue>>> consumeFunc =
          () => Task.Run(() => _consumer!.Consume(cancellationToken), cancellationToken);

        var consumeResult = await consumeFunc.RetryAsync(maxRetries: Constants.MAX_CONSUME_RETRIES,
                                            delayBetweenRetries: TimeSpan.FromSeconds(Constants.DELAY_IN_SECONDS_BETWEEN_RETRIES),
                                            cancellationToken: cancellationToken);

        Console.WriteLine($"Consume message from topic: {consumeResult.Topic} message: {consumeResult.Message.Serialize()}");
        return consumeResult;
    }
    public async Task CommitAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancelationToken)
    {
        if (_kafkaFactory.SubConfig.EnableAutoCommit) 
        {
            Console.WriteLine("Auto commit enabled.");
            return;
        }
        
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

    public async Task ConsumeWithRetriesAsync(Func<RetryableConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, CancellationToken cancellationToken = default!)
    {
        SubscribeToTopics();

        var tasks = new List<Task>();
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = await ConsumeAsync(cancellationToken);
                await ProcessConsumeResultAsync(consumeResult: consumeResult,
                                      onMessageReceived: onMessageReceived,
                                      tasks: tasks,
                                      cancellationToken: cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch(Exception ex)
            {
                Console.WriteLine($"An error occurred while trying to consume the message error: {ex.Message}.");
            }
        }

        HandleConsumerCancellation();
    }
    private void SubscribeToTopics()
    {
        var topics = GetSubscriptionTopics();
        Subscribe(topics);
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
    private async Task ProcessConsumeResultAsync(ConsumeResult<TKey, TValue> consumeResult, Func<RetryableConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, List<Task> tasks, CancellationToken cancellationToken)
    {
        if (consumeResult.IsPartitionEOF)
        {
            Console.WriteLine($"Consumer has reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}");

            await HandleEndOfPartitionAsync(tasks: tasks, cancellationToken: cancellationToken);
        }
        else
        {
            await ProcessReceivedMessageAsync(consumeResult, onMessageReceived, tasks, cancellationToken);
        }
    }
    private async Task ProcessReceivedMessageAsync(ConsumeResult<TKey, TValue> consumeResult, Func<RetryableConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, List<Task> tasks, CancellationToken cancellationToken)
    {
        if (_kafkaFactory.SubConfig.MaxConcurrentMessages == 0)
        {
            _ = Task.Run(async () => await TryProcessMessageAsync(consumeResult: consumeResult,
                                                                  onMessageReceived: onMessageReceived,
                                                                  cancellationToken: cancellationToken), cancellationToken: cancellationToken);
        }
        else
        {
            tasks.Add(TryProcessMessageAsync(consumeResult, onMessageReceived, cancellationToken));

            if (tasks.Count >= _kafkaFactory.SubConfig.MaxConcurrentMessages)
            {
                await ExecuteTasksAndClearAsync(tasks);
            }
        }
    }
    private static async Task ExecuteTasksAndClearAsync(List<Task> tasks)
    {
        await Task.WhenAll(tasks);
        tasks.Clear();
    }
    private async Task HandleEndOfPartitionAsync(List<Task> tasks, CancellationToken cancellationToken)
    {
        if (tasks.Any())
        {
            await ExecuteTasksAndClearAsync(tasks);
            return;
        }

        await Task.Delay(delay: TimeSpan.FromSeconds(_kafkaFactory.SubConfig.DelayPartitionEofMs),
                         cancellationToken: cancellationToken);
    }
    private async Task TryProcessMessageAsync(ConsumeResult<TKey, TValue> consumeResult, Func<RetryableConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        try
        {
            var retryableConsumeResult = new RetryableConsumeResult<TKey, TValue>(consumeResult);

            await ProcessMessageWithPotentialTimeoutAsync(retryableConsumeResult: retryableConsumeResult,
                                                          onMessageReceived: onMessageReceived,
                                                          cancellationToken: cancellationToken);

            if (retryableConsumeResult.ShouldRetry)
            {
                await HandleRetryAsync(consumeResult: consumeResult, cancellationToken: cancellationToken);
                return;
            }

            await CommitAsync(consumeResult: consumeResult,
                              cancelationToken: cancellationToken);

        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing message key: {consumeResult.Message.Key} timestamp: {consumeResult.Message.Timestamp} exception message: {ex.Message}.");

            await HandleRetryAsync(consumeResult: consumeResult,
                                   cancellationToken: cancellationToken);
        }
    }
    private async Task ProcessMessageWithPotentialTimeoutAsync(RetryableConsumeResult<TKey, TValue> retryableConsumeResult, Func<RetryableConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (_kafkaFactory.SubConfig.MessageProcessingTimeoutMs == 0)
        {
            await onMessageReceived(retryableConsumeResult, cancellationToken);
        }
        else
        {
            Func<CancellationToken, Task> taskToRun = ct => onMessageReceived(retryableConsumeResult, ct);

            await taskToRun.ExecuteWithTimeoutAsync(
                timeout: TimeSpan.FromMilliseconds(_kafkaFactory.SubConfig.MessageProcessingTimeoutMs),
                externalCancellationToken: cancellationToken);

          
        }
    }
    private async Task HandleRetryAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken)
    {
        var retryCount = consumeResult.Message.Headers.GetHeaderAs<int>(Constants.HEADER_NAME_RETRY_COUNT);

        Console.WriteLine($"Retry message processing retryCount: {retryCount}.");

        while (retryCount < _kafkaFactory.SubConfig.RetryLimit)
        {
            try
            {
                await PublishToRetryTopicAsync(consumeResult: consumeResult,
                                               retryCount: retryCount + 1,
                                               cancellationToken: cancellationToken);

                await CommitAsync(consumeResult: consumeResult,
                                  cancelationToken: cancellationToken);

                break;
            }
            catch (Exception retryEx)
            {
                Console.WriteLine($"Retry attempt {retryCount + 1} failed. Exception message: {retryEx.Message}");
                retryCount++;
            }
        }

        if (retryCount >= _kafkaFactory.SubConfig.RetryLimit)
        {
            await PublishToDeadLetterTopicAsync(kafkaMessage: consumeResult.Message, cancellationToken: cancellationToken);
        }

        await CommitAsync(consumeResult, cancellationToken);
        
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
            Console.WriteLine($"An error occurred while publishing message to dlq exception message: {ex.Message}.");
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                _consumer?.Close();
                _consumer?.Dispose();
                _producer?.Dispose();
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


