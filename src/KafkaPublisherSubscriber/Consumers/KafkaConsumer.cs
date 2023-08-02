using Confluent.Kafka;
using System.Threading.Tasks;
using System.Threading;
using System;
using KafkaPublisherSubscriber.Factories;

namespace KafkaPublisherSubscriber.Consumers
{

    public class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        public readonly IConsumer<TKey, TValue> _consumer;

        private readonly KafkaConsumerConfig _consumerSettings;

        public KafkaConsumer(KafkaConsumerConfig consumerSettings)
        {
            _consumerSettings = consumerSettings;
            _consumer = KafkaConnectionFactory.CreateConsumer<TKey, TValue>(consumerSettings);
        }

        public KafkaConsumerConfig Settings => _consumerSettings;

        public async Task<ConsumeResult<TKey, TValue>> Consume(CancellationToken cancellationToken)
        {
            return await Task.Run(() => _consumer.Consume(cancellationToken), cancellationToken);
        }

        public async Task Commit(ConsumeResult<TKey, TValue> consumeResult)
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

        public static KafkaConsumer<TKey, TValue> CreateInstance(Action<KafkaConsumerConfigBuilder> consumerConfigAction)
        {
            var consumerSettingsBuilder = new KafkaConsumerConfigBuilder();
            consumerConfigAction?.Invoke(consumerSettingsBuilder);
            var consumerSettings = consumerSettingsBuilder.Build();
            return new KafkaConsumer<TKey, TValue>(consumerSettings);
        }
    }
}
