using Confluent.Kafka;
using KafkaPublisherSubscriber.Configs;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KafkaPublisherSubscriber.HealthCheck
{
    public class KafkaPublisherSubscriberHealthCheck : IHealthCheck
    {
        private readonly Action<KafkaAdminConfig> _adminConfigAction;
        public KafkaPublisherSubscriberHealthCheck(Action<KafkaAdminConfig> adminConfigAction)
        {
            ArgumentNullException.ThrowIfNull(adminConfigAction, nameof(adminConfigAction));

            _adminConfigAction = adminConfigAction;
        }

        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {

            var adminConfig = new KafkaAdminConfig();
            _adminConfigAction(adminConfig);

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = adminConfig.BootstrapServers
            };

            if (adminConfig.IsCredentialsProvided)
            {
                adminClientConfig.SaslMechanism = SaslMechanism.ScramSha512;
                adminClientConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                adminClientConfig.SaslUsername = adminConfig.Username;
                adminClientConfig.SaslPassword = adminConfig.Password;
            }


            using var admin = new AdminClientBuilder(adminClientConfig).Build();

            try
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
                if(metadata.Brokers.Count == 0)
                {
                    return Task.FromResult(HealthCheckResult.Unhealthy("No Kafka brokers available."));
                }

                return Task.FromResult(HealthCheckResult.Healthy());
            }
            catch (Exception ex)
            {
                return Task.FromResult(HealthCheckResult.Unhealthy("Failed to connect to kafka.", ex));
            }
        }
    }
}
