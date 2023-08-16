using KafkaPublisherSubscriber.HealthCheck;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPublisherSubscriber.Extensions;

public static class ServicesCollectionExtension
{
  
    public static KafkaPublisherSubscriber AddKafkaBroker(this IServiceCollection services, string brokerName, string boststrapServer, string username = default!, string password = default!)
    {
        ArgumentNullException.ThrowIfNull(brokerName);

        services.AddHealthChecks()
            .AddCheck($"Kafka - {brokerName}", new KafkaPublisherSubscriberHealthCheck(config => {
                config.SetBootstrapServers(boststrapServer);
                if(username is not null && password is not null)
                {
                    config.SetCredentials(username, password);
                }
            }));

        return new KafkaPublisherSubscriber(services, boststrapServer, username, password);
    }

}
