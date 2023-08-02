using Confluent.Kafka;
using KafkaPublisherSubscriber.Factories;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaPublisherSubscriber.Producers
{

    public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private readonly KafkaProducerConfig _producerSettings;

        public KafkaProducer(KafkaProducerConfig producerSettings)
        {
            _producerSettings = producerSettings ?? throw new ArgumentNullException(nameof(producerSettings));
        }

        public KafkaProducerConfig Settings => _producerSettings;

        public async Task<DeliveryResult<TKey, TValue>> SendAsync(string topic, TValue message, TKey key = default, Headers headers = null)
        {
            var kafkaMessage = new Message<TKey, TValue>
            {
                Key = key,
                Value = message,
                Headers = headers
            };

            using (var producer = KafkaConnectionFactory.CreateProducer<TKey, TValue>(_producerSettings))
            {
                return await producer.ProduceAsync(topic, kafkaMessage);
            }
        }

        public async Task SendBatchAsync(string topic, IEnumerable<TValue> messages)
        {
            using (var producer = KafkaConnectionFactory.CreateProducer<TKey, TValue>(_producerSettings))
            {
                foreach (var message in messages)
                {
                    var result = await producer.ProduceAsync(topic, new Message<TKey, TValue> { Value = message });
                    Console.WriteLine($"Mensagem '{message}' enviada para partição: {result.Partition}, Offset: {result.Offset}");
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public static KafkaProducer<TKey, TValue> CreateInstance(Action<KafkaProducerConfigBuilder> producerConfigAction)
        {
            var producerSettingsBuilder = new KafkaProducerConfigBuilder();
            producerConfigAction?.Invoke(producerSettingsBuilder);
            var producerSettings = producerSettingsBuilder.Build();
            return new KafkaProducer<TKey, TValue>(producerSettings);
        }
    }

}
