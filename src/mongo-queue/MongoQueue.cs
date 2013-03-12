using System;
using System.Threading;
using Common.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace monQue
{
    public class MongoQueue<T> : IPublish<T> where T : class
    {
        private static ILog Log = LogManager.GetLogger("MongoQueue_" + typeof(T).Name);


        private readonly MongoDatabase _database;
        protected readonly MongoCollection<MongoMessage<T>> _queue;   // the collection for the messages
        protected readonly string _queueName = typeof(T).Name;        // name of collection (based on type name)

        private int _totalSent = 0;
        public int TotalSent { get { return _totalSent; } }

        public MongoQueue() : this(new MongoQueConfig()) { }

        public MongoQueue(MongoQueConfig config)
        {
            // our queue name will be the same as the message class
            _database = MongoDatabase.Create(config.ConnectionString);

            if (!_database.CollectionExists(_queueName))
            {
                try
                {
                    Log.InfoFormat("Creating queue '{0}' size {1}", _queueName, config.QueueSize);

                    var options = CollectionOptions
                        .SetCapped(true)        // use a capped collection so space is pre-allocated and re-used
                        .SetAutoIndexId(true)
                        .SetMaxSize(config.QueueSize); // limit the size of the collection and pre-allocated the space to this number of bytes

                    _database.CreateCollection(_queueName, options);
                    var col = _database.GetCollection(_queueName);
                    col.EnsureIndex( new[] { "Dequeued" } );
                    col.EnsureIndex(new[] { "Equeued" });
                }
                catch
                {
                    // assume that any exceptions are because the collection already exists ...
                }
            }

            // get the queue collection for our messages
            _queue = _database.GetCollection<MongoMessage<T>>(_queueName);
        }

        #region IPublish<T> Members

        public void Send(T message, QueueMode mode = QueueMode.PubSub)
        {
            // sending a message is easy - we just insert it into the collection 
            // it will be given a new sequential Id and also be written to the end (of the capped collection)
            Log.Debug(m => m("Sending {0} ({1}. Mode={2})", _queueName, message, mode));
            
            _queue.Insert(new MongoMessage<T>(message, mode));
        }

        #endregion

        #region ISubscribe<T> Members

       

       

        
        
        #endregion
       

    }

   
}