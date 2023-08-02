using System;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka;
using System.Text;
using KafkaPublisherSubscriber.Consumers;
using KafkaPublisherSubscriber.Producers;
using System.Collections.Generic;
using KafkaPublisherSubscriber.Extensions;

namespace KafkaPublisherSubscriber.Handlers
{

    public class KafkaMessageHandler<TKey, TValue> : IKafkaMessageHandler<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _kafkaConsumer;
        private readonly IKafkaProducer<TKey, TValue> _kafkaProducer;

        public KafkaMessageHandler(IKafkaConsumer<TKey, TValue> kafkaConsumer, IKafkaProducer<TKey, TValue> kafkaProducer)
        {
            _kafkaConsumer = kafkaConsumer ?? throw new ArgumentNullException(nameof(kafkaConsumer));
            _kafkaProducer = kafkaProducer ?? throw new ArgumentNullException(nameof(kafkaProducer));
        }

        public async Task Subscribe(Func<TValue, Task> onMessageReceived, CancellationToken cancellationToken)
        {
            var topics = new List<string>
            {
                _kafkaConsumer.Settings.Topic
            };

            if (!string.IsNullOrEmpty(_kafkaConsumer.Settings.TopicRetry))
            {
                topics.Add(_kafkaConsumer.Settings.TopicRetry);
            }

            _kafkaConsumer.Subscribe(topics.ToArray());

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = await _kafkaConsumer.Consume(cancellationToken);

                    try
                    {
                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine("Consumer has reached end of topic {Topic}, partition {Partition}, offset {OffSet}",
                                              consumeResult.Topic,
                                              consumeResult.Partition,
                                              consumeResult.Offset);
                            continue;
                        }


                        await onMessageReceived?.Invoke(consumeResult.Message.Value);
                        if (!_kafkaConsumer.Settings.EnableAutoCommit)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            await _kafkaConsumer.Commit(consumeResult); // Commit the offset since the message was successfully processed
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Consumer Cancelled!");
                        break; // Break the loop when the token is canceled
                    }
                    catch (Exception ex)
                    {
                        int retryCount = consumeResult.Message.Headers.GetRetryCountFromHeader();
                        await HandleError(_kafkaConsumer, consumeResult, ex, retryCount, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer Cancelled!");
            }
            finally
            {
                _kafkaConsumer?.Dispose();
            }
        }

        protected internal virtual async Task HandleError(IKafkaConsumer<TKey, TValue> consumer,
                                                          ConsumeResult<TKey, TValue> consumeResult,
                                                          Exception ex,
                                                          int retryCount,
                                                          CancellationToken cancellationToken)
        {
            // Log the error or perform any other error handling logic
            Console.WriteLine($"Error processing message: {consumeResult.Message.Value}. Exception: {ex}");

            // Retry the message if the retry count is less than the maximum allowed
            while (retryCount < _kafkaConsumer.Settings.MaxRetryAttempts)
            {
                try
                {
                    // Retry the message by using the consumer and producer interfaces
                    await consumer.Commit(consumeResult); // Commit the offset before retrying
                    await PublishToRetryTopicAsync(consumeResult.Message.Value, consumeResult.Message.Key, retryCount + 1);
                    break; // Exit the retry loop if the retry is successful
                }
                catch (Exception retryEx)
                {
                    // Log the retry error or perform any other retry error handling logic
                    Console.WriteLine($"Retry attempt {retryCount + 1} failed. Exception: {retryEx}");
                    retryCount++;
                }
            }

            if (retryCount >= _kafkaConsumer.Settings.MaxRetryAttempts)
            {
                // The message has reached the maximum retry attempts, publish it to the dead letter queue
                await PublishToDeadLetterTopicAsync(consumeResult.Message.Value);
            }

            if (!_kafkaConsumer.Settings.EnableAutoCommit)
            {
                // Commit the offset since the message was processed (either retried or sent to the dead letter queue)
                await consumer.Commit(consumeResult);
            }
        }

        private async Task PublishToRetryTopicAsync(TValue message, TKey key, int retryCount = 0)
        {
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes(retryCount.ToString()) }
            };

            await _kafkaProducer.SendAsync(_kafkaConsumer.Settings.TopicRetry, message, key, headers);
        }

        private async Task PublishToDeadLetterTopicAsync(TValue message)
        {
            await _kafkaProducer.SendAsync(_kafkaConsumer.Settings.TopicDeadLetter, message);
        }
    }
}
