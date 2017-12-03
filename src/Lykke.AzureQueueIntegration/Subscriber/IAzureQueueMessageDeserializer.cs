namespace Lykke.AzureQueueIntegration.Subscriber
{
    public interface IAzureQueueMessageDeserializer<out TModel>
    {
        TModel Deserialize(string data);
    }

}
