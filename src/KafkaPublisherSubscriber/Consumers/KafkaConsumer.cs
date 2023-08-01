using Confluent.Kafka;
using System.Threading.Tasks;
using System.Threading;
using System;
using KafkaPublisherSubscriber.Factories;

namespace KafkaPublisherSubscriber.Consumers
{
    public interface IKafkaConsumer : IDisposable
    {
        KafkaConsumerSettings Settings { get; }
        Task<ConsumeResult<Ignore, string>> Consume(CancellationToken cancellationToken);
        Task Commit(ConsumeResult<Ignore, string> consumeResult);
        void Subscribe(string[] topics);
    }


    public class KafkaConsumer : IKafkaConsumer
    {

        public readonly IConsumer<Ignore, string> _consumer;

        private readonly KafkaConsumerSettings _consumerSettings;

        public KafkaConsumer(KafkaConsumerSettings consumerSettings)
        {
            _consumerSettings = consumerSettings;
            _consumer = KafkaConnectionFactory.CreateConsumer(consumerSettings);
        }

        public KafkaConsumerSettings Settings { get { return _consumerSettings; } }

        public async Task<ConsumeResult<Ignore, string>> Consume(CancellationToken cancellationToken)
        {
            return await Task.Run(() => _consumer.Consume(cancellationToken), cancellationToken);
        }

        public async Task Commit(ConsumeResult<Ignore, string> consumeResult)
        {
            await Task.Run(() => _consumer.Commit(consumeResult));
        }


        public void Subscribe(string[] topics)
        {
            _consumer.Subscribe(topics);
        }

        public void Dispose()
        {
            _consumer?.Close();
            _consumer?.Dispose();
        }


        public static KafkaConsumer CreateInstance(Action<KafkaConsumerSettingsBuilder> consumerConfigAction)
        {
            var consumerSettingsBuilder = new KafkaConsumerSettingsBuilder();
            consumerConfigAction?.Invoke(consumerSettingsBuilder);
            var consumerSettings = consumerSettingsBuilder.Build();

            return new KafkaConsumer(consumerSettings);
        }
    }

}
