using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage;

namespace Lykke.AzureQueueIntegration.Subscriber
{

    public interface IAzureQueueMessageDeserializer<out TModel>
    {
        TModel Deserialize(string data);
    }

    public class AzureQueueSubscriber<TModel> : IStarter, IMessageConsumer<TModel>
    {
        private readonly string _applicationName;
        private readonly AzureQueueSettings _settings;
        private IAzureQueueMessageDeserializer<TModel> _deserializer;
        private ILog _log;

        public AzureQueueSubscriber(string applicationName, AzureQueueSettings settings)
        {
            _applicationName = applicationName;
            _settings = settings;
        }


        #region Configure

        public AzureQueueSubscriber<TModel> SetDeserializer(IAzureQueueMessageDeserializer<TModel> deserializer)
        {
            _deserializer = deserializer;
            return this;
        }

        public AzureQueueSubscriber<TModel> SetLogger(ILog log)
        {
            _log = log;
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

                    while (_task != null)
                    {


                        var messages = (await queue.GetMessagesAsync(31)).ToArray();

                        if (messages.Length == 0)
                            await Task.Delay(1000);
                        else
                            foreach (var message in messages)
                            {
                                var data = _deserializer.Deserialize(message.AsString);
                                await Task.WhenAll(_callbacks.Select(itm => itm(data)));
                                await queue.DeleteMessageAsync(message);
                            }

                    }

                }
                catch (Exception e)
                {
                    await _log.WriteErrorAsync(_applicationName, "TheTask", "", e);
                }

        }


        public AzureQueueSubscriber<TModel> Start()
        {
            if (_task != null)
                return this;


            if (_deserializer == null)
                throw new Exception("Deserializer required for: "+_applicationName);

            if (_log == null)
                throw new Exception("ILog required for: " + _applicationName);

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

        private readonly List<Func<TModel, Task>> _callbacks = new List<Func<TModel, Task>>();

        public void Subscribe(Func<TModel, Task> callback)
        {
            _callbacks.Add(callback);
        }

    }
}
