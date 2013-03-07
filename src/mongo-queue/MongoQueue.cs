using System;
using System.Threading;
using Common.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace monQue
{
    public class MongoQueue<T> : IPublish<T>, ISubscribe<T> where T : class
    {
        private static ILog Log = LogManager.GetCurrentClassLogger();

        const int SHORT_SLEEP_INTERVAL = 500; // 0.5 SEC POLL - acts as the minimal polling interval
        const int LONG_SLEEP_INTERVAL = 4000; // 4 SEC POLLING INTERVAL - when we have't seen event for more than a minute

        private readonly MongoDatabase _database;
        private readonly MongoCollection<MongoMessage<T>> _queue;   // the collection for the messages
        private readonly string _queueName = typeof(T).Name;        // name of collection (based on type name)
        private readonly DateTime _started = DateTime.UtcNow;
        
        private MongoCursorEnumerator<MongoMessage<T>> _enumerator; // our cursor enumerator
        private ObjectId _lastId = ObjectId.Empty;                  // the last _id read from the queue

        private int _totalReceived = 0;
        private int _totalSent = 0;
        public int TotalSent { get { return _totalSent; } }
        public int TotalReceived { get { return _totalReceived; } }

        public MongoQueue(string connectionString, long queueSize)
        {
            // our queue name will be the same as the message class
            _database = MongoDatabase.Create(connectionString);

            if (!_database.CollectionExists(_queueName))
            {
                try
                {
                    Log.InfoFormat("Creating queue '{0}' size {1}", _queueName, queueSize);

                    var options = CollectionOptions
                        .SetCapped(true)        // use a capped collection so space is pre-allocated and re-used
                        .SetAutoIndexId(true)
                        .SetMaxSize(queueSize); // limit the size of the collection and pre-allocated the space to this number of bytes

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

        /// <summary>
        /// Receives an event (or action) from queue. Does not change the message state. Used for PubSub (1 to Many - all gets the same messages) use cases
        /// </summary>
        /// <returns></returns>
        public T ReceiveEvent()
        {
            if (_enumerator == null)
                _enumerator = InitializeCursor();

            // running untill we have something to return
            while (true)
            {
                try
                {
                    // do we have a message waiting? 
                    // this may block on the server for a few seconds but will return as soon as something is available
                    var t1 = DateTime.UtcNow;
                    if (_enumerator.MoveNext())
                    {
                        // yes - record the current position and return it to the client
                        Interlocked.Increment(ref _totalReceived);
                        _lastId = _enumerator.Current.Id;

                        var current = _enumerator.Current;
                        var message = current.Message;
                        var delay = DateTime.UtcNow - _enumerator.Current.Enqueued;
                        Log.Debug(m => m("Received {0} after {1}", _queueName, delay));
                        return message;
                    }

                    // if the cursor is dead then we need to re-query, otherwise we just go back to iterating over it
                    if (_enumerator.IsDead)
                    {
                        Log.Debug("Cursor Dead");
                        _enumerator.Dispose();
                        _enumerator = InitializeCursor();
                    }

                    // If we are here it means that the server returned without items, usually it sits in a 2 seconds block on the server, but if initial request is empty then return immedietly.
                    // This is a safety mechanism to protect the mongo server from our DOS...(due to this cursor behavior, and from unknown bugs / 'behaviors')
                    var iterationTime = DateTime.UtcNow - t1;
                    if (iterationTime.TotalMilliseconds < SHORT_SLEEP_INTERVAL) // minimal polling interval : TODO - move to Config 
                        Thread.Sleep(SHORT_SLEEP_INTERVAL);
                }
                catch 
                {
                    // cursor died or was killed
                    _enumerator.Dispose();
                    _enumerator = InitializeCursor();
                }
            }
        }

        private MongoCursorEnumerator<MongoMessage<T>> InitializeCursor()
        {
            Log.Debug(m => m("Initializing Cursor from {0}", _lastId));
            IMongoQuery findFirstMessageQuery = (_lastId == ObjectId.Empty || _lastId == null) ?
                Query.GT("Enqueued", _started) : // First run
                Query.GT("_id", _lastId); // Rest of iterations
            var cursor = _queue
                .Find(findFirstMessageQuery)
                .SetFlags(
                    QueryFlags.AwaitData |
                    QueryFlags.NoCursorTimeout |
                    QueryFlags.TailableCursor
                )
                .SetSortOrder(SortBy.Ascending("$natural"));

            return (MongoCursorEnumerator<MongoMessage<T>>)cursor.GetEnumerator();
        }


        /// <summary>
        /// WorkQueue pattern.
        /// Receives a 'locked' action message. Each message shall be consumed by a single worker (thought there could be multiple workers - that will 'share the load')
        /// </summary>
        /// <returns></returns>
        public T ReceiveAction()
        {
            int iter = 0;
            
            
            while (true)
            {
                var found = _queue.FindAndModify(   Query.EQ("Dequeued", DateTime.MinValue),  //Query.And(Query.EQ("Dequeued", DateTime.MinValue), Query.EQ("MType",MongoMessageType.Action)) // used that to filter out NOOP and non action events. Removed for now since NOOP will be filtered by the dequeued property and events will be in different collection (future)
                                                    SortBy.Null, //SortBy.Ascending("_id"), //SortBy.Ascending("_id"),  //SortBy.Ascending("$natural"), // Removed - no need since natural order is the default and because it creates warning in Mongod. Needs to research more
                                                    Update.Set("Dequeued", DateTime.UtcNow),
                                                    false);

                if (found != null && found.ModifiedDocument != null)
                {
                    Interlocked.Increment(ref _totalReceived);
                    var retMsg = found.GetModifiedDocumentAs<MongoMessage<T>>();
                    _lastId = retMsg.Id; //TODO: do we need it??
                    iter = 0;
                    return retMsg.Message;
                }
                else // no available doc to receive. poll for events
                {
                    Thread.Sleep(++iter < 120 ? SHORT_SLEEP_INTERVAL: LONG_SLEEP_INTERVAL);
                    continue;
                    
                    //if (_enumerator == null)
                    //    _enumerator = InitializeCursorForWorkQueue();

                    //// there is no end when you need to sit and wait for messages to arrive
                    //while (true)
                    //{
                    //    try
                    //    {
                    //        // do we have a message waiting? 
                    //        // this may block on the server for a few seconds but will return as soon as something is available
                    //        if (_enumerator.MoveNext())
                    //        {
                    //            // yes - record the current position and return it to the client
                    //            //var unlockedMessage = _enumerator.Current.Message;

                    //            // Yeah - we got a message - let's try to get a 'lock' on it (by starting the loop)
                    //            _enumerator.Dispose();
                    //            _enumerator = null;
                    //            break; // Should we kill the long poller now ?
                    //        }

                    //        // if the cursor is dead then we need to re-query, otherwise we just go back to iterating over it
                    //        if (_enumerator.IsDead)
                    //        {
                    //            Log.Debug("Cursor Dead");
                    //            _enumerator.Dispose();
                    //            _enumerator = InitializeCursorForWorkQueue();
                    //        }
                    //    }
                    //    catch
                    //    {
                    //        // cursor died or was killed
                    //        if (_enumerator!= null) _enumerator.Dispose();
                    //        _enumerator = InitializeCursorForWorkQueue();
                    //    }
                    //}
                }
            }


            
            
        }
        #endregion
       

    }
}