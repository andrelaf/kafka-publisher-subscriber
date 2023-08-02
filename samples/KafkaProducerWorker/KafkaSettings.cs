namespace ProducerWorker
{
    public class KafkaSettings
    {
        public required string BootstrapServers { get; set; }
        public required string Topic { get; set; }
    }
}
