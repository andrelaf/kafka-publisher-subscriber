namespace KafkaPublisherSubscriber
{
    public static class Constants
    {
        public const string HEADER_NAME_RETRY_COUNT = "RetryCount";

        public const int MAX_MESSAGE_SEND_RETRIES = 10;
        public const int MAX_MESSAGE_SEND_RETRIES_WITH_ENABLE_IDEMPOTENCE = 3;
        public const int MAX_IN_FLIGHT_WITH_ENABLE_IDEMPOTENCE = 5;
        public const int MIN_DELAY_IN_SECONDS_ENABLE_PARTITION_EOF = 5;
    }
}
