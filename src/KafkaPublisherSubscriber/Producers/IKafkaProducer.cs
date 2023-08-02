using Confluent.Kafka;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaPublisherSubscriber.Producers
{
    public interface IKafkaProducer<TKey, TValue>
    {
        KafkaProducerConfig Settings { get; }
        Task<DeliveryResult<TKey, TValue>> SendAsync(string topic, TValue message, TKey key = default, Headers headers = null);
        Task SendBatchAsync(string topic, IEnumerable<TValue> messages);
    }
}
