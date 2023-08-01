using ProducerWorker;
using KafkaPublisherSubscriber;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddKafkaProducer(producerConfigAction =>
        {
            producerConfigAction.WithBootstrapServers("localhost:9092");
            producerConfigAction.WithTopic("my-topic");
        });

        services.AddHostedService<Worker>();
    })
    .Build();

host.Run();
