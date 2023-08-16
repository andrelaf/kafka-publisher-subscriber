using Microsoft.Extensions.Logging;

namespace KafkaPublisherSubscriber.Extensions;

public static class TaskExtension
{
    public static async Task ExecuteWithTimeoutAsync(this Func<CancellationToken, Task> taskFunc, TimeSpan timeout, CancellationToken externalCancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
        var delayTask = Task.Delay(timeout, linkedCts.Token);
        var actionTask = taskFunc(linkedCts.Token);

        var completedTask = await Task.WhenAny(actionTask, delayTask);
        if (completedTask == delayTask)
        {
            // Se a delayTask for a tarefa que foi concluída, isso significa que o tempo limite foi atingido.
            linkedCts.Cancel();  // Cancela a actionTask
            Console.WriteLine($"Processing message took too long and was time out after {timeout} milliseconds.");
            throw new TimeoutException($"The operation exceeded the timeout of {timeout}.");
        }

        // Aguarda e propaga as exceções, se a actionTask falhar.
        await actionTask;
    }

    public static async Task<T> RetryAsync<T>(this Task<T> task, int maxRetries, TimeSpan delayBetweenRetries, CancellationToken cancellationToken)
    {
        int currentRetry = 0;

        while (currentRetry <= maxRetries)
        {
            try
            {
                return await task;
            }
            catch (Exception ex)
            {
                if (currentRetry >= maxRetries) throw;

                currentRetry++;
                Console.WriteLine($"Error encountered. Retry {currentRetry} of {maxRetries}. Exception: {ex.Message}");

                await Task.Delay(delayBetweenRetries, cancellationToken);
            }
        }

        throw new InvalidOperationException("Failed after maximum retries.");
    }

}
