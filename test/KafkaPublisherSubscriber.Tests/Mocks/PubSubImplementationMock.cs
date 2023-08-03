using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.PubSub;

namespace KafkaPublisherSubscriber.Tests.Mocks
{

    public interface IPubSubImplementationMock : IKafkaPubSub<string, string> {}

    public class PubSubImplementationMock : KafkaPubSub<string, string>, IPubSubImplementationMock
    {
        public PubSubImplementationMock(IKafkaFactory kafkaFactory) : base(kafkaFactory)
        {
                
        }
    }
}
