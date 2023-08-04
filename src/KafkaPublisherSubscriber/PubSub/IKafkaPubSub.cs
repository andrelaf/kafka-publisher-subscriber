using Confluent.Kafka;
using KafkaPublisherSubscriber.Results;

namespace KafkaPublisherSubscriber.PubSub
{
    public interface IKafkaPubSub { }
    public interface IKafkaPubSub<TKey, TValue> : IKafkaPubSub, IDisposable
    {
        Task<DeliveryResult<TKey, TValue>> SendAsync(TValue message, TKey key = default!, Headers headers = default!);
        Task<BatchSendResult<TKey, TValue>> SendBatchAsync(IEnumerable<Message<TKey, TValue>> messages);
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken);
        Task CommitAsync(ConsumeResult<TKey, TValue> consumeResult);
        void Subscribe(string[] topics);
        Task TryConsumeWithRetryFlowAsync(Func<ConsumeResult<TKey, TValue>, Task> onMessageReceived, CancellationToken cancellationToken);
    }
}
