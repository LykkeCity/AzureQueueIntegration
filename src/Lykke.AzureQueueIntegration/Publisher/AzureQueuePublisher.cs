using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration.Publisher
{

    public class AzureQueuePublisher<TModel> : TimerPeriod, IMessageProducer<TModel>
    {
        private readonly AzureQueueSettings _settings;
        private IAzureQueueSerializer<TModel> _serializer;

        public AzureQueuePublisher(string applicationName, AzureQueueSettings settings)
            : base(applicationName, 1000)
        {
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();
        }

        #region Config

        public new AzureQueuePublisher<TModel> SetLogger(ILog log)
        {
            base.SetLogger(log);
            return this;
        }

        public AzureQueuePublisher<TModel> SetSerializer(IAzureQueueSerializer<TModel> serializer)
        {
            _serializer = serializer;
            return this;
        }


        #endregion

        private CloudQueue _cloudQueue;

        public override async Task Execute()
        {

            if (_cloudQueue == null)
                _cloudQueue = await _settings.GetQueueAsync();


            var message = _queue.Dequeue();
            if (message == null)
                return;

            var dataToQueue = _serializer.Serialize(message.Item);
            await _cloudQueue.AddMessageAsync(new CloudQueueMessage(dataToQueue));
            message.Compliete();
        }


        public new AzureQueuePublisher<TModel> Start()
        {
            base.Start();
            return this;
        }


        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }

    }

}
