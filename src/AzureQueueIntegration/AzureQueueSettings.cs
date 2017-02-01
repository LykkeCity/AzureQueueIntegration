using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureQueueIntegration
{
    public class AzureQueueSettings
    {

        public string ConnectionString { get; set; }
        public string QueueName { get; set; }
    }
}
