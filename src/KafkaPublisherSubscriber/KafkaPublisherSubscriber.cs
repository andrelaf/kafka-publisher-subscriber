using KafkaPublisherSubscriber.Configs;
using KafkaPublisherSubscriber.Factories;
using KafkaPublisherSubscriber.PubSub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KafkaPublisherSubscriber;

[ExcludeFromCodeCoverage]
public class KafkaPublisherSubscriber
{
    private readonly IServiceCollection _services;
    private readonly string _bootstrapServer;
    private readonly string _username;
    private readonly string _password;
    public KafkaPublisherSubscriber(IServiceCollection services, string bootstrapServer, string username, string password)
    {
        _services = services;
        _bootstrapServer = bootstrapServer;
        _username = username;
        _password = password;
    }

    public KafkaPublisherSubscriber AddKafkaPubSub<TKey, TValue>(Action<KafkaSubConfig> subConfigAction = default!,
                                                           Action<KafkaPubConfig> pubConfigAction = default!)
    {
        return AddKafkaImplementation<IKafkaPubSub<TKey, TValue>, KafkaPubSub<TKey, TValue>>(subConfigAction, pubConfigAction);
    }

    private KafkaPublisherSubscriber AddKafkaImplementation<TService, TImplementation>(Action<KafkaSubConfig> subConfigAction = default!,
                                                                                 Action<KafkaPubConfig> pubConfigAction = default!)
   where TService : class, IKafkaPubSub
   where TImplementation : class, TService
    {
        if (subConfigAction is null && pubConfigAction is null)
        {
            throw new ArgumentException($"{nameof(subConfigAction)} and/or {nameof(pubConfigAction)} are required.");
        }


        KafkaSubConfig? subConfig = null;
        if (subConfigAction is not null)
        {
            subConfig = new KafkaSubConfig();
            subConfigAction.Invoke(subConfig);
            SetBrokerConfig(subConfig);
            KafkaValidatorConfig.ValidateSubConfig(subConfig);
        }

        KafkaPubConfig? pubConfig = null;
        if (pubConfigAction != null)
        {
            pubConfig = new KafkaPubConfig();
            pubConfigAction.Invoke(pubConfig);
            SetBrokerConfig(pubConfig);
            KafkaValidatorConfig.ValidatePubConfig(pubConfig);

            if(pubConfig is null)
            {
                pubConfig = new KafkaPubConfig();
                SetBrokerConfig(pubConfig);
            }
        }

        var boorstrapServers = (pubConfig?.BootstrapServers ?? subConfig?.BootstrapServers);
        var username = (pubConfig?.Username ?? subConfig?.Username);
        var password = (pubConfig?.Password ?? subConfig?.Username);

        _services.AddSingleton<TService, TImplementation>(s =>
        {

            var logger = s.GetRequiredService<ILogger<IKafkaPubSub>>();
            return (TImplementation)Activator.CreateInstance(typeof(TImplementation), new KafkaFactory(logger, subConfig!, pubConfig!))!;
        });

        return this;
    }


    private void SetBrokerConfig(KafkaConfig config)
    {
        config.SetBootstrapServers(_bootstrapServer);
        if(_username is not null && _password is not null)
        {
            config.SetCredentials(_username, _password);
        }
    }
}
