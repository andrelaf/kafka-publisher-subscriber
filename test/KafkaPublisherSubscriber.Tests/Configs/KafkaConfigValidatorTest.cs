﻿using KafkaPublisherSubscriber.Configs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaPublisherSubscriber.Tests.Configs
{
    public class KafkaConfigValidatorTests
    {
        [Fact]
        public void ValidatePubConfig_WhenBootstrapServersIsNull_ThrowsArgumentNullException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetTopic("testTopic");

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidatePubConfig_WhenTopicIsNull_ThrowsArgumentNullException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetBootstrapServers("localhost:9092");

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidatePubConfig_WhenMessageSendMaxRetriesGreaterThanTen_ThrowsArgumentException()
        {
            var pubConfig = new KafkaPubConfig();
            pubConfig.SetBootstrapServers("localhost:9092");
            pubConfig.SetTopic("testTopic");
            pubConfig.SetMessageSendMaxRetries(11);

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidatePubConfig(pubConfig));

            Assert.IsType<ArgumentException>(ex);
        }


        [Fact]
        public void ValidateSubConfig_WhenBootstrapServersIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenTopicIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetGroupId("testGroup");

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenGroupIdIsNull_ThrowsArgumentNullException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentNullException>(ex);
        }

        [Fact]
        public void ValidateSubConfig_WhenEnablePartitionEofIsEnabledAndDelayInSecondsPartitionEofIsLessThanOne_ThrowsArgumentException()
        {
            var subConfig = new KafkaSubConfig();
            subConfig.SetBootstrapServers("localhost:9092");
            subConfig.SetTopic("testTopic");
            subConfig.SetGroupId("testGroup");
            subConfig.SetEnablePartitionEof(true);
            subConfig.SetDelayInSecondsPartitionEof(0);

            Exception ex = Record.Exception(() => KafkaConfigValidator.ValidateSubConfig(subConfig));

            Assert.IsType<ArgumentException>(ex);
        }
    }
}
