using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.AzureQueueIntegration.Subscriber
{
    public class AzureQueueSubscriber<TModel> : TimerPeriod, IMessageConsumer<TModel>
    {
        private readonly AzureQueueSettings _settings;
        private readonly List<Func<TModel, Task>> _callbacks = new List<Func<TModel, Task>>();

        private CloudQueue _cloudQueue;
        private IAzureQueueMessageDeserializer<TModel> _deserializer;

        public AzureQueueSubscriber(string applicationName, AzureQueueSettings settings)
            :base(applicationName, 1000)
        {
            _settings = settings;

            DisableTelemetry();
        }

        public AzureQueueSubscriber(
            string applicationName,
            AzureQueueSettings settings,
            bool disableTelemetry)
            : base(applicationName, 1000)
        {
            _settings = settings;

            if (disableTelemetry)
                DisableTelemetry();
        }

        #region Configure

        public AzureQueueSubscriber<TModel> SetDeserializer(IAzureQueueMessageDeserializer<TModel> deserializer)
        {
            _deserializer = deserializer;
            return this;
        }

        public new AzureQueueSubscriber<TModel> SetLogger(ILog log)
        {
            base.SetLogger(log);
            return this;
        }

        #endregion

        public override async Task Execute()
        {

            if (_cloudQueue == null)
                _cloudQueue = await _settings.GetQueueAsync();

            var messages = (await _cloudQueue.GetMessagesAsync(31)).ToArray();

            if (messages.Length == 0)
                return;

            foreach (var message in messages)
            {
                var data = _deserializer.Deserialize(message.AsString);
                await Task.WhenAll(_callbacks.Select(itm => itm(data)));
                await _cloudQueue.DeleteMessageAsync(message);
            }
        }

        public new AzureQueueSubscriber<TModel> Start()
        {
            base.Start();
            return this;
        }

        public void Subscribe(Func<TModel, Task> callback)
        {
            _callbacks.Add(callback);
        }
    }
}
