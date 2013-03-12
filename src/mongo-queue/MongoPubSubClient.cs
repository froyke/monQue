using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Bson;
using Common.Logging;
using System.Threading.Tasks;
using System.Threading;
using MongoDB.Driver.Builders;

namespace monQue
{
    class MongoPubSubClient<T> : MongoQueue<T>, ISubscribe<T> where T : class
    {
        const int SHORT_SLEEP_INTERVAL = 500; // 0.5 SEC POLL - acts as the minimal polling interval
        const int LONG_SLEEP_INTERVAL = 4000; // 4 SEC POLLING INTERVAL - when we have't seen event for more than a minute

        private static ILog Log = LogManager.GetLogger("MongoPubSubClinet_" + typeof(T).Name);

        private readonly DateTime _started = DateTime.UtcNow;
        private MongoCursorEnumerator<MongoMessage<T>> _enumerator; // our cursor enumerator
        private ObjectId _lastId = ObjectId.Empty;                  // the last _id read from the queue
        private int _totalReceived = 0;
        public int TotalReceived { get { return _totalReceived; } }
        List<Action<T>> _listeners = new List<Action<T>>();
        Task _runner = null;

        public MongoPubSubClient(MongoQueConfig config) : base(config) { }
        

        
        // Register the handler and return immedietly
        public void ReceiveEvents(Action<T> listener)
        {
            _listeners.Add(listener); // todo - thread safe / lock

            if (_runner == null)  // todo - thread safe / lock
            {
                _runner = Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        T msg = ReceiveEvent();
                        Log.Debug(string.Format("[ReceiveEvents] Message arrived. publishing to {0} local subscribers. Msg={1}: " ,_listeners.Count,  msg.ToString()));
                        // Distribute event to all subscribers in the process. 
                        // TODO:Consider spawning several tasks.One faulty (blocking) reader might cause starvation 
                        try
                        {
                            _listeners.ForEach(l => l(msg)); // todo: message immutability / cloning. TODO: Error handling (receivers isolation)
                        }
                        catch (Exception ex)
                        {
                            Log.Error("PubSubQueue: Unhandled exception was thrown by a listener.", ex);
                        }
                    }
                }, TaskCreationOptions.LongRunning); // consider using threads instead.
            }
        }



        /// <summary>
        /// Receives an event (or action) from queue. Does not change the message state. Used for PubSub (1 to Many - all gets the same messages) use cases
        /// </summary>
        /// <returns></returns>
        private T ReceiveEvent()
        {
            if (_enumerator == null)
                _enumerator = InitializeCursor();

            // running untill we have something to return
            while (true)
            {
                var t1 = DateTime.UtcNow;
                try
                {
                    // do we have a message waiting? 
                    // this may block on the server for a few seconds but will return as soon as something is available

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
                    //This is a safety mechanism to protect the mongo server from our DOS...(due to this cursor behavior, and from unknown bugs / 'behaviors')
                    var iterationTime = DateTime.UtcNow - t1;
                    if (iterationTime.TotalMilliseconds < SHORT_SLEEP_INTERVAL) // minimal polling interval : TODO - move to Config 
                        Thread.Sleep(SHORT_SLEEP_INTERVAL);


                }
                catch
                {
                    // cursor died or was killed
                    if(_enumerator != null)  
                        _enumerator.Dispose();
                    _enumerator = InitializeCursor();

                    //This is a safety mechanism to protect the mongo server from our DOS...
                    var iterationTime = DateTime.UtcNow - t1;
                    if (iterationTime.TotalMilliseconds < SHORT_SLEEP_INTERVAL) // minimal polling interval : TODO - move to Config 
                        Thread.Sleep(SHORT_SLEEP_INTERVAL);
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

    }
}
