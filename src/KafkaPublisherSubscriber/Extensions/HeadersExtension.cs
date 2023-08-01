using Confluent.Kafka;
using System.Text;

namespace KafkaPublisherSubscriber.Extensions
{
    public static class HeadersExtension
    {
        public static int GetRetryCountFromHeader(this Headers headers)
        {
            int retryCount = 0;

            if (headers != null && headers.TryGetLastBytes("RetryCount", out byte[] retryCountBytes))
            {
                string retryCountString = Encoding.UTF8.GetString(retryCountBytes);
                int.TryParse(retryCountString, out retryCount);
            }

            return retryCount;
        }
    }
}
