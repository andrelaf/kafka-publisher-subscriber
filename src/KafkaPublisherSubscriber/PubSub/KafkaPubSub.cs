using KafkaPublisherSubscriber.Factories;

namespace KafkaPublisherSubscriber.PubSub
{

    public class KafkaPubSub<TKey, TValue> : KafkaPubSubBase<TKey, TValue>
    {
        public KafkaPubSub(IKafkaFactory kafkaFactory) : base(kafkaFactory) { }
    }

}


