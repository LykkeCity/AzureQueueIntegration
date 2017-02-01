using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureQueueIntegration.Publisher
{

    public interface IAzureQueueSerializer<in TModel>
    {
        string Serialize(TModel model);
    }

    public class AzureQueuePublisher<TModel> : IStarter, IMessageProducer<TModel>
    {
        private readonly string _applicationName;
        private readonly AzureQueueSettings _settings;

        private IAzureQueueSerializer<TModel> _serializer;
        private ILog _log;

        public AzureQueuePublisher(string applicationName, AzureQueueSettings settings)
        {
            _applicationName = applicationName;
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();
        }

        #region Config

        private AzureQueuePublisher<TModel> SetLogger(ILog log)
        {
            _log = log;
            return this;
        }

        private AzureQueuePublisher<TModel> SetSerializer(IAzureQueueSerializer<TModel> serializer)
        {
            _serializer = serializer;
            return this;
        }


        #endregion


        private Task _task;

        private async Task TheTask()
        {

            while (_task != null)
                try
                {

                    var storageAccount = CloudStorageAccount.Parse(_settings.ConnectionString);
                    var queueClient = storageAccount.CreateCloudQueueClient();
                    var queue = queueClient.GetQueueReference(_settings.QueueName);
                    await queue.CreateIfNotExistsAsync();


                    while (true)
                    {
                        var message = _queue.Dequeue();
                        if (message == null)
                        {
                            if (_task == null)
                                break;

                            await Task.Delay(1000);
                            continue;
                        }

                        var dataToQueue = _serializer.Serialize(message.Item);
                        await queue.AddMessageAsync(new CloudQueueMessage(dataToQueue));
                        message.Compliete();
                    }

                }
                catch (Exception e)
                {
                    await _log.WriteErrorAsync(_applicationName, "TheTask", "", e);
                }
        }


        public AzureQueuePublisher<TModel> Start()
        {
            if (_task != null)
                return this;

            if (_log == null)
                throw new Exception("ILog required for: "+_applicationName);

            if (_serializer == null)
                throw new Exception("IAzureQueueSerializer required for: " + _applicationName);

            _task = TheTask();

            return this;
        }

        void IStarter.Start()
        {
            Start();
        }


        public void Stop()
        {
            if (_task == null)
                return;

            var task = _task;
            _task = null;
            task.Wait();
        }

        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }
    }
}
