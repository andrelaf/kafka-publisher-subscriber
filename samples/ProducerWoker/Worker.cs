using KafkaPublisherSubscriber.PubSub;
using ProducerWoker.Messages;

namespace ProducerWoker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaPubSub<string, OrderMessage> _kafkaPubSub;
    public Worker(ILogger<Worker> logger, IKafkaPubSub<string, OrderMessage> kafkaPubSub)
    {
        _logger = logger;
        _kafkaPubSub = kafkaPubSub;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int messagesToSend = 1000;
        for (int i = 1; i <= messagesToSend; i++)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            string key = i.ToString();
            _ = await _kafkaPubSub.SendAsync(message: new OrderMessage
            {
                Description = "Order 1",
                Value = 1000
            },
            key: key,
            cancellationToken: stoppingToken);
        }

        _kafkaPubSub.Dispose();
    }
}