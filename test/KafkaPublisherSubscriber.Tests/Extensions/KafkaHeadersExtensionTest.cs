using Confluent.Kafka;
using System.Text;
using KafkaPublisherSubscriber.Extensions;

namespace KafkaPublisherSubscriber.Tests.Extensions
{
    public class KafkaHeadersExtensionTest
    {
        [Fact]
        public void GetHeaderAs_HeadersContainValidInteger_ReturnsInteger()
        {
            // Arrange
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes("3") }
            };

            // Act
            int retryCount = headers.GetHeaderAs<int>("RetryCount");

            // Assert
            Assert.Equal(3, retryCount);
        }

        [Fact]
        public void GetHeaderAs_HeadersContainInvalidInteger_ReturnsZero()
        {
            // Arrange
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes("invalid_retry_count") }
            };

            // Act
            int retryCount = headers.GetHeaderAs<int>("RetryCount");

            // Assert
            Assert.Equal(0, retryCount);
        }

        [Fact]
        public void GetHeaderAs_HeadersDoNotContainInteger_ReturnsZero()
        {
            // Arrange
            var headers = new Headers();

            // Act
            int retryCount = headers.GetHeaderAs<int>("RetryCount");

            // Assert
            Assert.Equal(0, retryCount);
        }
    }
}
