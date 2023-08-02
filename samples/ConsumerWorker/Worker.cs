using KafkaPublisherSubscriber.Handlers;

namespace ConsumerWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IKafkaMessageHandler<string, string> _kafkaMessageHandler;

        public Worker(ILogger<Worker> logger, IKafkaMessageHandler<string, string> kafkaMessageHandler)
        {
            _logger = logger;
            _kafkaMessageHandler = kafkaMessageHandler;
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