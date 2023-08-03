using Confluent.Kafka;
using System.Text.Json;
using System.Text;

namespace KafkaPublisherSubscriber.Serializers;

public class JsonSerializerUtf8<T> : ISerializer<T>
{
    private readonly JsonSerializerOptions jsonOptions;
    private readonly Encoding encoder;

    public JsonSerializerUtf8()
    {
        jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };

        encoder = Encoding.UTF8;
    }

    public byte[] Serialize(T data, SerializationContext context)
    {
        return encoder.GetBytes(JsonSerializer.Serialize(data, jsonOptions));
    }
}