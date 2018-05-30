using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration.Publisher
{
    public class AzureQueuePublisher<TModel> : TimerPeriod, IMessageProducer<TModel>
    {
        private readonly AzureQueueSettings _settings;
        private readonly ILog _log;
        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        private CloudQueue _cloudQueue;
        private IAzureQueueSerializer<TModel> _serializer;

        [Obsolete("Use ctor with logFactory")]
        public AzureQueuePublisher(string applicationName, AzureQueueSettings settings)
            : base(applicationName, 1000)
        {
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();

            DisableTelemetry();
        }

        [Obsolete("Use ctor with logFactory")]
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

        public AzureQueuePublisher(
            [NotNull] ILogFactory logFactory,
            [NotNull] IAzureQueueSerializer<TModel> serializer,
            [NotNull] string publisherName,
            [NotNull] AzureQueueSettings settings,
            TimeSpan sendPeriod,
            bool disableTelemetry = true)
            
            : base(sendPeriod, logFactory, publisherName)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _log = logFactory?.CreateLog(this, publisherName) ?? throw new ArgumentNullException(nameof(logFactory));

            _settings = new AzureQueueSettings
            {
                QueueName = _settings.QueueName.ToLower(),
                ConnectionString = _settings.ConnectionString
            };
            
            if (disableTelemetry)
                DisableTelemetry();
        }

        #region Config

        [Obsolete("Use ctor to set logFactory")]
        public new AzureQueuePublisher<TModel> SetLogger(ILog log)
        {
            base.SetLogger(log);
            return this;
        }

        [Obsolete("Use ctor to set serializer")]
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
                _log?.WriteError(
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

            Execute().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }
    }
}
