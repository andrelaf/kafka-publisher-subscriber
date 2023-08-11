using Confluent.Kafka;
using KafkaPublisherSubscriber.Results;

namespace KafkaPublisherSubscriber.PubSub
{
    public interface IKafkaPubSub { }
    public interface IKafkaPubSub<TKey, TValue> : IKafkaPubSub, IDisposable
    {
        Task<DeliveryResult<TKey, TValue>> SendAsync(TValue message, TKey key = default!, Headers headers = default!, CancellationToken cancellationToken = default!);
        Task<BatchSendResult<TKey, TValue>> SendBatchAsync(IEnumerable<Message<TKey, TValue>> kafkaMessages, CancellationToken cancellationToken = default!);
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken);
        Task CommitAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancelationToken);
        void Subscribe(string[] topics);
        Task ConsumeWithRetryFlowAsync(Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> onMessageReceived, CancellationToken cancellationToken = default!);
    }
}
