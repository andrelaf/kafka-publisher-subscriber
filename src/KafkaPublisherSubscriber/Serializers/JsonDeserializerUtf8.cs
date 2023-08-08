using Confluent.Kafka;
using System.Text.Json;
using System.Text;

namespace KafkaPublisherSubscriber.Serializers;

public class JsonDeserializerUtf8<T> : IDeserializer<T>
{
    private readonly JsonSerializerOptions jsonOptions;
    private readonly Encoding encoder;

    public JsonDeserializerUtf8()
    {
        jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        };

        encoder = Encoding.UTF8;
    }

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return default!;

        var jsonString = encoder.GetString(data.ToArray());

        try
        {
            return JsonSerializer.Deserialize<T>(jsonString, jsonOptions)!;
        }
        catch(JsonException ex)
        {
            Console.WriteLine($"Error deserialializing message: {ex.Message}.");
            return default!;
        }
    }
}