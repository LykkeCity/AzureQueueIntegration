namespace Lykke.AzureQueueIntegration.Publisher
{
    public interface IAzureQueueSerializer<in TModel>
    {
        string Serialize(TModel model);
    }

}
