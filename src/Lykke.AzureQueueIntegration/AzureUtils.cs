using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration
{
    public static class AzureQueueUtils
    {
        public static async Task<CloudQueue> GetQueueAsync(this AzureQueueSettings settings, 
            bool fireNForgetExistenceCheck = false)
        {
            var storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(settings.QueueName);

            var checkQueueExistTsk = queue.CreateIfNotExistsAsync();

            if (!fireNForgetExistenceCheck)
            {
                await checkQueueExistTsk;
            }

            await queue.CreateIfNotExistsAsync();
            return queue;
        }
    }
}
