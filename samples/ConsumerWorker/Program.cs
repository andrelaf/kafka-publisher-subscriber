using ConsumerWorker;
using KafkaPublisherSubscriber.Extensions;

internal class Program
{
    private static void Main(string[] args)
    {

        IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                services.AddKafkaProducerAndConsumer<string, string>(consumerConfigAction =>
                {
                    consumerConfigAction.WithBootstrapServers("localhost:9092");
                    consumerConfigAction.WithTopic("my-topic");
                    consumerConfigAction.WithGroupId("group-test");
                    consumerConfigAction.WithEnableAutoCommit(false);
                    // Add any other consumer settings as needed
                },
            producerConfigAction =>
            {
                producerConfigAction.WithBootstrapServers("localhost:9092");
                producerConfigAction.WithTopic("my-topic");
                // Add any other producer settings as needed
            });

                services.AddHostedService<Worker>();
            })
            .Build();

        host.Run();
    }
}