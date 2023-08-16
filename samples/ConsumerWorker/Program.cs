using Confluent.Kafka;
using ConsumerWorker;
using ConsumerWorker.Messages;
using KafkaPublisherSubscriber.Extensions;
using KafkaPublisherSubscriber.Settings;

IHost host = Host.CreateDefaultBuilder(args)
   .ConfigureServices((hostContext, services) =>
   {
       IConfiguration configuration = hostContext.Configuration;

       var kafkaBrokerSetgings = configuration.GetSection(nameof(KafkaBrokerSettings)).Get<Dictionary<string, KafkaBrokerSettings>>();

       ArgumentNullException.ThrowIfNull(kafkaBrokerSetgings);

       foreach (var brokerSettings in kafkaBrokerSetgings)
       {
           var kafkaPublisherSubscriber = services.AddKafkaBroker(brokerSettings.Key,
                                                                  brokerSettings.Value.BootstrapServers,
                                                                  brokerSettings.Value.Username!,
                                                                  brokerSettings.Value.Password!);

           foreach (var topicSettings in brokerSettings.Value.Topics)
           {
               kafkaPublisherSubscriber.AddKafkaPubSub<string, OrderMessage>(
                   subConfigAction: config =>
                   {
                       config.SetTopic(topicSettings.Key);
                       config.SetTopicRetry(topicSettings.Value.TopicRetry!);
                       config.SetTopicDeadLetter(topicSettings.Value.TopicDeadLetter!);
                       config.SetGroupId(topicSettings.Value.GroupId!);
                       config.SetRetryLimit(topicSettings.Value.RetryLimit!);
                       config.SetMaxConcurrentMessages(topicSettings.Value.RetryLimit!);
                       config.SetDelayPartitionEofMs(topicSettings.Value.RetryLimit!);
                       config.SetAutoOffsetReset(AutoOffsetReset.Earliest);
                       config.SetRetryTopicSubscriptionEnabled();
                       config.SetMessageProcessingTimeoutMs(topicSettings.Value.MessageProcessingTimeoutMilliseconds);
                   });
           }

       }

       services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
