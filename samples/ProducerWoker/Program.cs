using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Settings;
using ProducerWoker;
using ProducerWoker.Messages;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        IConfiguration configuration = hostContext.Configuration;

        var kafkaBrokerSetgings = configuration.GetSection(nameof(KafkaBrokerSettings)).Get<Dictionary<string, KafkaBrokerSettings>>();

        ArgumentNullException.ThrowIfNull(kafkaBrokerSetgings);

        foreach (var brokerSettings in kafkaBrokerSetgings)
        {
            var kafkaPublisherSubscriber = services.AddKafkaBroker(brokerSettings.Key, brokerSettings.Value.BootstrapServers, brokerSettings.Value.Username!, brokerSettings.Value.Password!);

            foreach (var topicSettings in brokerSettings.Value.Topics)
            {
                kafkaPublisherSubscriber.AddKafkaPubSub<string, OrderMessage>(
                    pubConfigAction: config =>
                    {
                        config.SetIdempotenceEnabled();
                        config.SetTopic(topicSettings.Key);
                    });
            }

        }


        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
