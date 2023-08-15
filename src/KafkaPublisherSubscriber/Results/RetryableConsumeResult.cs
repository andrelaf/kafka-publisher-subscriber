using Confluent.Kafka;

namespace KafkaPublisherSubscriber.Results;

public class RetryableConsumeResult<TKey, TValue> : ConsumeResult<TKey, TValue>
{
    public bool ShouldRetry { get; private set; } = false;
    public RetryableConsumeResult(ConsumeResult<TKey, TValue> consumeResult)
    {
        Message = consumeResult.Message;
        Topic = consumeResult.Topic;
        Partition = consumeResult.Partition;
        Offset = consumeResult.Offset;
        TopicPartitionOffset = consumeResult.TopicPartitionOffset;
        IsPartitionEOF = consumeResult.IsPartitionEOF;
    }

    public void MarkForRetry()
    {
        ShouldRetry = true;
    }
}
