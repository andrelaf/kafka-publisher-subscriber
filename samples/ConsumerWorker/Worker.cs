using KafkaPublisherSubscriber.Handlers;

namespace ConsumerWorker
{

    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        private readonly IKafkaMessageHandler _kafkaMessageHandler;

        public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _kafkaMessageHandler = serviceProvider.GetRequiredService<IKafkaMessageHandler>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _kafkaMessageHandler.Subscribe((message) =>
            {
                _logger.LogInformation("Received message: {message} - at: {time}.", message, DateTimeOffset.Now);
                return Task.CompletedTask;
            }, stoppingToken);
        }
    }
}