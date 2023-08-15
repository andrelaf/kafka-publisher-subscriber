using System.Text.Json;

namespace KafkaPublisherSubscriber.Extensions;

public static class JsonExtension
{
    public static string Serialize<T>(this T data)
    {
        if (data is null) return null!;

        return JsonSerializer.Serialize(data);
    }
}
