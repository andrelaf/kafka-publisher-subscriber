using Confluent.Kafka;
using KafkaPublisherSubscriber.Factories;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaPublisherSubscriber.Producers
{
    public interface IKafkaProducer
    {
        KafkaProducerSettings Settings { get; }
        Task<DeliveryResult<Null, string>> SendMessageAsync(string topic, string message);
        Task<DeliveryResult<Null, string>> SendMessageAsync(string topic, string message, Headers headers);
        Task SendManyMessagesAsync(string topic, IEnumerable<string> messages);
    }
    public class KafkaProducer : IKafkaProducer
    {
        private readonly KafkaProducerSettings _producerSettings;
        public KafkaProducer(KafkaProducerSettings producerSettings)
        {
            _producerSettings = producerSettings ?? throw new ArgumentNullException(nameof(producerSettings));
        }
        public KafkaProducerSettings Settings { get { return _producerSettings;  } }

        public async Task<DeliveryResult<Null, string>> SendMessageAsync(string topic, string message)
        {
            using (var producer = KafkaConnectionFactory.CreateProducer(_producerSettings))
            {
                return await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
            }
        }
        public async Task<DeliveryResult<Null, string>> SendMessageAsync(string topic, string message, Headers headers)
        {
            var kafkaMessage = new Message<Null, string>
            {
                Value = message,
                Headers = headers
            };

            using (var producer = KafkaConnectionFactory.CreateProducer(_producerSettings))
            {
                return await producer.ProduceAsync(topic, kafkaMessage);
            }

        }
        public async Task SendManyMessagesAsync(string topic, IEnumerable<string> messages)
        {
            using (var producer = KafkaConnectionFactory.CreateProducer(_producerSettings))
            {
                foreach (var message in messages)
                {
                    var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Mensagem '{message}' enviada para partição: {result.Partition}, Offset: {result.Offset}");
                }
            }
        }


        public static KafkaProducer CreateInstance(Action<KafkaProducerSettingsBuilder> producerConfigAction)
        {
            var producerSettingsBuilder = new KafkaProducerSettingsBuilder();
            producerConfigAction?.Invoke(producerSettingsBuilder);
            var producerSettings = producerSettingsBuilder.Build();
            return new KafkaProducer(producerSettings);
        }
    }
}
