namespace KafkaPublisherSubscriber.Configs;

public abstract class KafkaConfig
{
    public string? BootstrapServers { get; private set; }
    public string? Topic { get; private set; }
    public string Username { get; private set; } = string.Empty;
    public string Password { get; private set; } = string.Empty;
    public bool IsCredentialsProvided { get; private set; } = false;
    internal void SetBootstrapServers(string bootstrapServers)
    {
        BootstrapServers = bootstrapServers;
    }
    internal void SetCredentials(string username, string password)
    {
        Username = username;
        Password = password;
        IsCredentialsProvided = true;
    }

    public void SetTopic(string topic)
    {
        Topic = topic;
    }
}
