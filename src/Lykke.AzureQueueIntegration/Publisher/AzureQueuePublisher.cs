using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration.Publisher
{
    public class AzureQueuePublisher<TModel> : TimerPeriod, IMessageProducer<TModel>
    {
        private readonly AzureQueueSettings _settings;
        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        private CloudQueue _cloudQueue;
        private IAzureQueueSerializer<TModel> _serializer;

        public AzureQueuePublisher(string applicationName, AzureQueueSettings settings)
            : base(applicationName, 1000)
        {
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();

            DisableTelemetry();
        }

        public AzureQueuePublisher(
            string applicationName,
            AzureQueueSettings settings,
            bool disableTelemetry)
            : base(applicationName, 1000)
        {
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();

            if (disableTelemetry)
                DisableTelemetry();
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

        public override async Task Execute()
        {
            if (_cloudQueue == null)
                _cloudQueue = await _settings.GetQueueAsync();

            QueueWithConfirmation<TModel>.QueueItem message = null;
            try
            {
                do
                {
                    message = _queue.Dequeue();

                    if (message == null)
                    {
                        break;
                    }

                    var dataToQueue = _serializer.Serialize(message.Item);
                    await _cloudQueue.AddMessageAsync(new CloudQueueMessage(dataToQueue));
                    message.Compliete();
                } while (true);
            }
            catch (Exception ex)
            {
                Log?.WriteError(
                    $"{GetComponentName()}:{_settings.QueueName}",
                    message == null || message.Item == null ? string.Empty : message.Item.ToJson(),
                    ex);
                throw;
            }
        }

        public new AzureQueuePublisher<TModel> Start()
        {
            base.Start();
            return this;
        }

        public override void Stop()
        {
            base.Stop();

            Execute().GetAwaiter().GetResult();
        }

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }
    }
}
