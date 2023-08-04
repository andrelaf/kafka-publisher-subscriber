using Confluent.Kafka;

namespace KafkaPublisherSubscriber.Results
{
    public class BatchSendResult<TKey, TValue>
    {
        public List<DeliveryResult<TKey, TValue>> Successes { get; set; } = new();
        public List<(Message<TKey, TValue> message, Exception exception)> Failures { get; set; } = new();
    }
}
