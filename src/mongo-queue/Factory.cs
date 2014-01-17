using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Configuration;

namespace monQue
{
    /// <summary>
    /// The factory should be a single instance per process!!!
    /// The c'tor is public mainly for unitest reasons.
    /// 
    /// </summary>
    public class MonQueFactory
    {
        private static readonly Lazy<MonQueFactory> instanceHolder =
            new Lazy<MonQueFactory>(() => new MonQueFactory());
        
        public static MonQueFactory Instance
        {
            get { return instanceHolder.Value; }
        }
        
        private  ConcurrentDictionary<string, object> _queues = new ConcurrentDictionary<string, object>();

        public IPublish<T> GetMessagesPublisher<T>() where T : class
        {
            return new MongoQueue<T>();
        }

        public void RegisterEventListener<T>(Action<T> listener) where T: class
        {
            // We cache PubSub Queues and connect new registrations to existing ones (of the same message type)
            // There could be multiple listeners for the same message type
            object desiredQ;
            string qName = typeof(T).Name;
            if (!_queues.TryGetValue(qName, out desiredQ))
            {
                desiredQ = new MongoPubSubClient<T>(new MongoQueConfig());
                _queues[qName] = desiredQ;
            }
            var pubsubQ =  desiredQ as ISubscribe<T>;
            pubsubQ.ReceiveEvents(listener);
        }

        public void RegisterWorker<T>(Action<T> work) where T : class
        {
            // No caching of mongo workers on the same process.
            // TODO: throw if another worker was already registered. ??? (but what about multi processor machines? we might want to run a few workers of the same type...)
            var mongoWorker = new MongoWorker<T>(new MongoQueConfig());
            mongoWorker.RegisterWorker(work);
        }      
    }

    public class MongoQueConfig
    {
        public string ConnectionString    { get; set; }
        public long   QueueSize { get; set; }
        public string Database { get; set; }
        public MongoQueConfig()
        {
            try
            {
                ConnectionString = ConfigurationManager.ConnectionStrings["mongo-queue"].ConnectionString;
                Database = (string)ConfigurationManager.AppSettings["queue-name"];
            }
            catch
            {
                ConnectionString = "mongodb://localhost/monQue";
            }
            try
            {
                QueueSize = long.Parse(ConfigurationManager.AppSettings["mongo-queue.size"]);
            }
            catch
            {
                QueueSize = 4194304; // 2 ^ 22; //4MB
            }
        }
    }
}
