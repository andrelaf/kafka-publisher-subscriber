using Confluent.Kafka;
using System.Text;
using KafkaPublisherSubscriber.Extensions;

namespace KafkaPublisherSubscriber.Tests.Extensions
{
    public class HeadersExtensionTests
    {
        [Fact]
        public void GetRetryCountFromHeader_HeadersContainValidRetryCount_ReturnsRetryCount()
        {
            // Arrange
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes("3") }
            };

            // Act
            int retryCount = headers.GetRetryCountFromHeader();

            // Assert
            Assert.Equal(3, retryCount);
        }

        [Fact]
        public void GetRetryCountFromHeader_HeadersContainInvalidRetryCount_ReturnsZero()
        {
            // Arrange
            var headers = new Headers
            {
                { "RetryCount", Encoding.UTF8.GetBytes("invalid_retry_count") }
            };

            // Act
            int retryCount = headers.GetRetryCountFromHeader();

            // Assert
            Assert.Equal(0, retryCount);
        }

        [Fact]
        public void GetRetryCountFromHeader_HeadersDoNotContainRetryCount_ReturnsZero()
        {
            // Arrange
            var headers = new Headers();

            // Act
            int retryCount = headers.GetRetryCountFromHeader();

            // Assert
            Assert.Equal(0, retryCount);
        }
    }
}
