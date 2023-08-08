namespace KafkaPublisherSubscriber.Configs
{
    public abstract class KafkaConfig
    {
        public string? BootstrapServers { get; private set; }
        public string? Topic { get; private set; }
        public string Username { get; private set; } = string.Empty;
        public string Password { get; private set; } = string.Empty;
        public bool IsCredentialsProvided { get; private set; } = false;
        public void SetBootstrapServers(string bootstrapServers)
        {
            BootstrapServers = bootstrapServers;
        }
        public void SetTopic(string topic)
        {
            Topic = topic;
        }
        public void SetCredentials(string username, string password)
        {
            Username = username;
            Password = password;
            IsCredentialsProvided = true;
        }
    }
}
