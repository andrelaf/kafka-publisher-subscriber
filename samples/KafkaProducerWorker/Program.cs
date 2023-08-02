using ProducerWorker;
using KafkaPublisherSubscriber;

IHost host = Host.CreateDefaultBuilder(args)
    .UseEnvironment("Staging")
    .ConfigureServices((hostContext, services) =>
    {

        IConfiguration configuration = hostContext.Configuration;

        var kafkaSettings = configuration.GetSection(nameof(KafkaSettings)).Get<KafkaSettings>();

        ArgumentNullException.ThrowIfNull(kafkaSettings);

        services.AddKafkaProducer<string, string>(producerConfigAction =>
        {
            producerConfigAction.WithBootstrapServers(kafkaSettings.BootstrapServers);
            producerConfigAction.WithTopic(kafkaSettings.Topic);
        });

        services.AddHostedService<Worker>();
    })
    .Build();

host.Run();
