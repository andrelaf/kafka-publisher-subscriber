using KafkaPublisherSubscriber.Producers;

namespace KafkaProducerExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var producerConfigAction = new Action<KafkaProducerSettingsBuilder>(builder =>
            {
                builder.WithBootstrapServers("localhost:9092");
                builder.WithTopic("my-topic");
            });

            var producer = KafkaProducer.CreateInstance(producerConfigAction);

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            try
            {
                int messageCounter = 1;

                while (!cancellationToken.IsCancellationRequested)
                {
                    var message = $"Test Message {messageCounter}";

                    var deliveryReport = await producer.SendMessageAsync(producer.Settings.Topic, message);

                    Console.WriteLine($"Message '{message}' sent to topic '{deliveryReport.Topic}' at partition {deliveryReport.Partition} and offset {deliveryReport.Offset}");

                    messageCounter++;

                    await Task.Delay(1000); // Delay between producing messages
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Producer cancelled!");
            }
        }
    }

  
}