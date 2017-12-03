using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration
{
    public static class AzureQueueUtils
    {
        public static async Task<CloudQueue> GetQueueAsync(this AzureQueueSettings settings)
        {
            var storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(settings.QueueName);
            await queue.CreateIfNotExistsAsync();
            return queue;
        }
    }
}
