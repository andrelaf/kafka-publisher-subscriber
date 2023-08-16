using ConsumerWorker.Messages;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.PubSub;
using KafkaPublisherSubscriber.Results;

namespace ConsumerWorker;

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

        stoppingToken.Register(() => {
            _logger.LogInformation("Worker is stopping.");
        });

 
        await _kafkaPubSub.ConsumeWithRetriesAsync(onMessageReceived: ProcessMessageAsync, cancellationToken: stoppingToken);
        
    }

    private async Task ProcessMessageAsync(RetryableConsumeResult<string, OrderMessage> result, CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation(message: "Order received description: {Description} value: {Value}", result.Message.Value.Description, result.Message.Value.Value);

            await Task.Delay(1000, stoppingToken);

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error ocurred while consuming messasges.");

            result.TryAgain();
        }
    }
}