using Confluent.Kafka;
using System.Text.Json;

namespace KafkaPublisherSubscriber.Serializers;

public class JsonSerializerUtf8<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}