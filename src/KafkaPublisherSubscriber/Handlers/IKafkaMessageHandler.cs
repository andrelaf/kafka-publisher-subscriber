using System;
using System.Threading.Tasks;
using System.Threading;

namespace KafkaPublisherSubscriber.Handlers
{
    public interface IKafkaMessageHandler<TKey, TValue>
    {
        Task Subscribe(Func<TValue, Task> onMessageReceived, CancellationToken cancellationToken);
    }
}
