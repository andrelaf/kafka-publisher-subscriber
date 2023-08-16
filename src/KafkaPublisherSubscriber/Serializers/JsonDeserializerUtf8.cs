using Confluent.Kafka;
using System.Text.Json;
using System.Text;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using KafkaPublisherSubscriber.PubSub;

namespace KafkaPublisherSubscriber.Serializers;

[ExcludeFromCodeCoverage]
public class JsonDeserializerUtf8<T> : IDeserializer<T>
{
    private readonly ILogger<IKafkaPubSub> _logger;
    private readonly JsonSerializerOptions jsonOptions;
    private readonly Encoding encoder;

    public JsonDeserializerUtf8(ILogger<IKafkaPubSub> logger)
    {
        _logger = logger;
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
            _logger.LogError(ex, "Error deserialializing message: {jsonString}.",jsonString);
            Console.WriteLine();
            return default!;
        }
    }
}