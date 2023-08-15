using Confluent.Kafka;
using System.Text;

namespace KafkaPublisherSubscriber.Extensions;

public static class KafkaHeadersExtension
{
    public static T? GetHeaderAs<T>(this Headers headers, string headerKey)
    {
        if (headers != null && headers.TryGetLastBytes(headerKey, out byte[] headerBytes))
        {
            string headerString = Encoding.UTF8.GetString(headerBytes);

            try
            {
                return (T)Convert.ChangeType(headerString, typeof(T));
            }
            catch
            {
                return default;
            }
        }

        return default;
    }

    public static void AddOrUpdate(this Headers headers, string headerKey, byte[] headerValue)
    {
        if(headers.TryGetLastBytes(headerKey, out _))
        {
            headers.Remove(headerKey);
        }

        headers.Add(headerKey, headerValue);
    }
}