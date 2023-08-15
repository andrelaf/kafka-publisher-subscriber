namespace KafkaPublisherSubscriber.Utilities;

public static class TimeoutTaskRunner
{
    public static async Task ExecuteWithTimeoutAsync(Func<CancellationToken, Task> taskFunc, TimeSpan timeout, CancellationToken externalCancellationToken)
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

}

