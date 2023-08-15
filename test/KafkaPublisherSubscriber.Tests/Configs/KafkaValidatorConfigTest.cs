using KafkaPublisherSubscriber.Configs;

namespace KafkaPublisherSubscriber.Tests.Configs
{
    public class KafkaValidatorConfigTest
    {
        [Fact]
        public void ValidatePubConfig_WhenBootstrapServersIsNull_ThrowsArgumentNullException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetTopic("testTopic");

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidatePubConfig_WhenTopicIsNull_ThrowsArgumentNullException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetBootstrapServers("localhost:9092");

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidatePubConfig_WhenMessageSendMaxRetriesGreaterThanTen_ThrowsArgumentException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetBootstrapServers("localhost:9092");
            pubConfig.SetTopic("testTopic");
            pubConfig.SetMessageSendMaxRetries(11);

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentException>(ex);
        }


        [Fact]
        public void ValidateSubConfig_WhenBootstrapServersIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenTopicIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetGroupId("testGroup");

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenGroupIdIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");

            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenEnablePartitionEofIsEnabledAndDelayInSecondsPartitionEofIsLessThanOne_ThrowsArgumentException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");
            subConfig.SetDelayIPartitionEofMs(0);


            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentException>(ex);
        }


        [Fact]
        public void ValidateCredentials_WhenCredentialsAreRequiredButUsernameIsEmpty_ThrowsArgumentException()
        {
            // Arrange
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");
            subConfig.SetDelayIPartitionEofMs(5);
            subConfig.SetCredentials(string.Empty, "password123");

            // Act
            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            // Assert
            Assert.IsType<ArgumentException>(ex);
        }

        [Fact]
        public void ValidateCredentials_WhenCredentialsAreRequiredButPasswordIsEmpty_ThrowsArgumentException()
        {
            // Arrange
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");
            subConfig.SetDelayIPartitionEofMs(0);
            subConfig.SetCredentials("user123", string.Empty);

            // Act
            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            //Assert
            Assert.IsType<ArgumentException>(ex);
        }

        [Fact]
        public void ValidateCredentials_WhenCredentialsAreValid_DoesNotThrowException()
        {
            // Arrange
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");
            subConfig.SetDelayIPartitionEofMs(5);
            subConfig.SetCredentials("user123", "validPassword"); // Provide a valid password

            // Act
            Exception ex = Record.Exception(() => KafkaValidatorConfig.ValidateSubConfig(subConfig));

            // Assert
            Assert.Null(ex);  // No exception should be thrown for valid credentials
        }

    }
}
