using KafkaPublisherSubscriber.Results;

namespace KafkaPublisherSubscriber.Extensions;

public static class RetryableConsumeResultExtension
{
    public static void TryAgain<TKey, TValue>(this RetryableConsumeResult<TKey, TValue> retryableConsumeResult)
    {
        retryableConsumeResult.MarkForRetry();
    }

}
