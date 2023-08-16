namespace ConsumerWorker.Messages;


public class OrderMessage
{
    public required string Description { get; set; }
    public decimal Value { get; set; }
}
