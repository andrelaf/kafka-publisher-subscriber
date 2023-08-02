using Confluent.Kafka;
using System;
using System.Threading.Tasks;
using System.Threading;

namespace KafkaPublisherSubscriber.Consumers
{
    public interface IKafkaConsumer<TKey, TValue> : IDisposable
    {
        KafkaConsumerConfig Settings { get; }
        Task<ConsumeResult<TKey, TValue>> Consume(CancellationToken cancellationToken);
        Task Commit(ConsumeResult<TKey, TValue> consumeResult);
        void Subscribe(string[] topics);
    }
}
