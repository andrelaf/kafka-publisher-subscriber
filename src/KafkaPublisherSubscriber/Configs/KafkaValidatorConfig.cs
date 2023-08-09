using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber.Configs;

[ExcludeFromCodeCoverage]
public static class KafkaValidatorConfig
{
    public static void ValidatePubConfig(KafkaPubConfig pubConfig)
    {
        ArgumentNullException.ThrowIfNull(pubConfig?.BootstrapServers);
        ArgumentNullException.ThrowIfNull(pubConfig?.Topic);

        if (pubConfig.MessageSendMaxRetries < Constants.MIN_MESSAGE_SEND_RETRIES || pubConfig.MessageSendMaxRetries > Constants.MAX_MESSAGE_SEND_RETRIES)
        {
            throw new ArgumentException($"The minimum value allowed for the {nameof(pubConfig.MessageSendMaxRetries)} property is {Constants.MIN_MESSAGE_SEND_RETRIES} and the maximum is {Constants.MAX_MESSAGE_SEND_RETRIES}. Current vaue: {pubConfig.MessageSendMaxRetries}.");
        }

        if (pubConfig.EnableIdempotence)
        {

            if (pubConfig.MaxInFlight < Constants.MIN_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE || pubConfig.MaxInFlight > Constants.MAX_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE)
            {
                throw new ArgumentException($"When {nameof(pubConfig.EnableIdempotence)} is enabled, the minimum value allowed for the {nameof(pubConfig.MaxInFlight)} property is {Constants.MIN_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE} and the maximum is {Constants.MAX_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE}. Current vaue: {pubConfig.MaxInFlight}.");
            }

            if (pubConfig.MessageSendMaxRetries > Constants.MAX_MESSAGE_SEND_RETRIES_WITH_ENABLE_IDEMPOTENCE)
            {
                throw new ArgumentException($"When {nameof(pubConfig.EnableIdempotence)} is enabled, the maximum value allowed for the {nameof(pubConfig.MessageSendMaxRetries)} property is {Constants.MAX_MESSAGE_SEND_RETRIES_WITH_ENABLE_IDEMPOTENCE}. Current vaue: {pubConfig.MessageSendMaxRetries}.");
            }


            if (pubConfig.Acks != Acks.All)
            {
                throw new ArgumentException($"When {nameof(pubConfig.EnableIdempotence)} is enabled, the value for {nameof(pubConfig.Acks)} property is required Acks.All. Current vaue: {pubConfig.Acks}.");
            }
        }

        ValidateCredentials(pubConfig.IsCredentialsProvided, pubConfig.Username, pubConfig.Password);

    }

   
    public static void ValidateSubConfig(KafkaSubConfig subConfig)
    {
        ArgumentNullException.ThrowIfNull(subConfig?.BootstrapServers);
        ArgumentNullException.ThrowIfNull(subConfig?.Topic);
        ArgumentNullException.ThrowIfNull(subConfig?.GroupId);
        ArgumentNullException.ThrowIfNull(subConfig?.ConsumerLimit);

        if (subConfig.EnablePartitionEof && subConfig.DelayInSecondsPartitionEof < Constants.MIN_DELAY_IN_SECONDS_ENABLE_PARTITION_EOF)
        {
            throw new ArgumentException($"When {nameof(subConfig.EnablePartitionEof)} is enabled, the {nameof(subConfig.DelayInSecondsPartitionEof)} property value must be greater than or equal to {Constants.MIN_DELAY_IN_SECONDS_ENABLE_PARTITION_EOF}. Current vaue: {subConfig.DelayInSecondsPartitionEof}.");
        }

        if (subConfig.EnableRetryTopicSubscription && subConfig.TopicRetry is null)
        {
            throw new ArgumentException($"When {nameof(subConfig.EnablePartitionEof)} is enabled, the value of {nameof(subConfig.TopicRetry)} property is required. Current vaue: {subConfig.TopicRetry}.");
        }

        ValidateCredentials(subConfig.IsCredentialsProvided, subConfig.Username, subConfig.Password);
    }

    private static void ValidateCredentials(bool isCredentialsProvide, string username, string password)
    {
        if (isCredentialsProvide && (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password)))
        {
            throw new ArgumentException($"Username and Password are required.");
        }
    }

}
