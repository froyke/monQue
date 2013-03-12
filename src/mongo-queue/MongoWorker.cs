using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Common.Logging;
using MongoDB.Driver.Builders;
using System.Threading;
using System.Threading.Tasks;

namespace monQue
{
    class MongoWorker<T> : MongoQueue<T>, IWorkerQueue<T> where T : class
    {
        const int SHORT_SLEEP_INTERVAL = 500; // 0.5 SEC POLL - acts as the minimal polling interval
        const int LONG_SLEEP_INTERVAL = 4000; // 4 SEC POLLING INTERVAL - when we have't seen event for more than a minute

        private static ILog Log = LogManager.GetLogger("MongoWorker_" + typeof(T).Name);

        private int _totalReceived = 0;
        public int TotalReceived { get { return _totalReceived; } }
        private Guid _uniqueWorkerId = Guid.NewGuid();

        public MongoWorker(MongoQueConfig config) : base(config) { }
        

        /// <summary>
        /// WorkQueue pattern.
        /// Receives a 'locked' action message. Each message shall be consumed by a single worker (thought there could be multiple workers - that will 'share the load')
        /// </summary>
        /// <returns></returns>
        private T ReceiveAction()
        {
            int iter = 0;

            while (true)
            {
                try
                {
                    var found = _queue.FindAndModify(Query.EQ("Dequeued", DateTime.MinValue),  //Query.And(Query.EQ("Dequeued", DateTime.MinValue), Query.EQ("MType",MongoMessageType.Action)) // used that to filter out NOOP and non action events. Removed for now since NOOP will be filtered by the dequeued property and events will be in different collection (future)
                                                        SortBy.Null, //SortBy.Ascending("_id"), //SortBy.Ascending("_id"),  //SortBy.Ascending("$natural"), // Removed - no need since natural order is the default and because it creates warning in Mongod. Needs to research more
                                                        Update.Set("Dequeued", DateTime.UtcNow),
                                                        false);

                    if (found != null && found.ModifiedDocument != null)
                    {
                        Interlocked.Increment(ref _totalReceived);
                        var retMsg = found.GetModifiedDocumentAs<MongoMessage<T>>();
                        iter = 0;
                        return retMsg.Message;
                    }
                    else
                    {
                        // no available doc to receive. Wait and then poll
                        Thread.Sleep(++iter < 120 ? SHORT_SLEEP_INTERVAL : LONG_SLEEP_INTERVAL);
                    }
                }
                catch
                {
                    //  ERROR OCCURED. Wait and then poll. TODO proper logging and unrecoverable error detection and restart / fail
                    Thread.Sleep(LONG_SLEEP_INTERVAL);
                }
            }
        }


        public void RegisterWorker(Action<T> worker)
        {
            Action mainWorkerLoop = () =>
            {
                while (true)
                {
                    T job = ReceiveAction();
                    //Console.WriteLine("worker Id:{0} , got message", _uniqueWorkerId);
                    try
                    {
                        worker(job);
                        // FUTURE/TODO: Add ack at this point
                    }
                    catch (Exception ex)
                    {
                        Log.Error("WorkerQueue: Unhandled exception was thrown by the job worker.", ex);
                        // FUTURE/TODO: add errors to the message/ increase Error count
                    }
                }
            };
            
            Task.Factory.StartNew(mainWorkerLoop, TaskCreationOptions.LongRunning);
        }
    }
}
