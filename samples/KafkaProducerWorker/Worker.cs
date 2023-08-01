using KafkaPublisherSubscriber.Producers;

namespace ProducerWorker
{
    public class Worker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;

        public Worker(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var producer = _serviceProvider.GetRequiredService<IKafkaProducer>();

            int messagesToSend = 1000;
            for (int i = 1; i <= messagesToSend; i++)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                var message = $"Test Message {i}";
                var deliveryReport = await producer.SendMessageAsync(producer.Settings.Topic, message);

                Console.WriteLine($"Message '{message}' sent to topic '{deliveryReport.Topic}' at partition {deliveryReport.Partition} and offset {deliveryReport.Offset}");

                await Task.Delay(100, stoppingToken); // Delay between sending messages
            }
        }
    }
}