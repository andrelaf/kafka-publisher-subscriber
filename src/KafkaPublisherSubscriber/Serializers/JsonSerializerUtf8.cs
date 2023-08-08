using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace KafkaPublisherSubscriber.Serializers;

[ExcludeFromCodeCoverage]
public class JsonSerializerUtf8<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}