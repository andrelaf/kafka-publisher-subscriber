using Confluent.Kafka;
using KafkaPublisherSubscriber.Configs;

namespace KafkaPublisherSubscriber.Factories;

public interface IKafkaFactory
{
    public KafkaSubConfig SubConfig { get; }
    public KafkaPubConfig PubConfig { get; }
    IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>();
    IProducer<TKey, TValue> CreateProducer<TKey, TValue>();
    Message<TKey, TValue> CreateKafkaMessage<TKey, TValue>(TValue message, TKey key = default!, Headers headers = default!);
}
