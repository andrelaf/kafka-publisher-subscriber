using Confluent.Kafka;

namespace KafkaPublisherSubscriber.Configs
{
    public static class KafkaValidatorConfig
    {
        public static void ValidatePubConfig(KafkaPubConfig pubConfig)
        {
            ArgumentNullException.ThrowIfNull(pubConfig?.BootstrapServers);
            ArgumentNullException.ThrowIfNull(pubConfig?.Topic);

            if(pubConfig.MessageSendMaxRetries > 10)
            {
                throw new ArgumentException($"Maximum value allowed for the MessageSendMaxRetries property is {Constants.MAX_MESSAGE_SEND_RETRIES}. Current vaue: {pubConfig.MessageSendMaxRetries}.");
            }

            if (pubConfig.EnableIdempotence)
            {
                if (pubConfig.MessageSendMaxRetries > 3)
                {
                    throw new ArgumentException($"When EnableIdempotence is enabled, the maximum value allowed for the MessageSendMaxRetriesMaxInFlight property is {Constants.MAX_MESSAGE_SEND_RETRIES_WITH_ENABLE_IDEMPOTENCE}. Current vaue: {pubConfig.MessageSendMaxRetries}.");
                }

                if (pubConfig.MaxInFlight > 5)
                {
                    throw new ArgumentException($"When EnableIdempotence is enabled, the maximum value allowed for the MaxInFlight property is {Constants.MAX_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE}. Current vaue: {pubConfig.MaxInFlight}.");
                }

                if (pubConfig.Acks != Acks.All)
                {
                    throw new ArgumentException($"When EnableIdempotence is enabled, the value allowed for the Acks property is Acks.All. Current vaue: {pubConfig.Acks}.");
                }

            }

        }

        public static void ValidateSubConfig(KafkaSubConfig subConfig)
        {
            ArgumentNullException.ThrowIfNull(subConfig?.BootstrapServers);
            ArgumentNullException.ThrowIfNull(subConfig?.Topic);
            ArgumentNullException.ThrowIfNull(subConfig?.GroupId);
            ArgumentNullException.ThrowIfNull(subConfig?.ConsumerLimit);

            if (subConfig.EnablePartitionEof && subConfig.DelayInSecondsPartitionEof < 1)
            {
                throw new ArgumentException($"When EnablePartitionEof is enabled, the DelayInSecondsPartitionEof property value must be greater than or equal to {Constants.MIN_DELAY_IN_SECONDS_ENABLE_PARTITION_EOF}. Current vaue: {subConfig.DelayInSecondsPartitionEof}.");
            }
        }
    }

}
