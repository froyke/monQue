using monQue;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Configuration;
using System.Linq;
using MongoDB.Driver;
using System.Threading.Tasks;
using System.Threading;


using MongoDB.Bson;

namespace test_mongo_queue
{
    
    
    /// <summary>
    ///This is a test class for MongoQueueTest and is intended
    ///to contain all MongoQueueTest Unit Tests
    ///</summary>
    [TestClass()]
    public class MongoQueueTest
    {
        private MonQueFactory _factory;

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        [TestInitialize()]
        public void MyTestInitialize()
        {
            DropCollection(typeof(TestMessage).Name);
            _factory = null;
            _factory = new monQue.MonQueFactory();
            Thread.Sleep(500); // I dont know if drop collection is sync call and if DB is not still doing stuff after call return...This will make tests timing more accurate
        }
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        [TestMethod]
        public void SimpleSendRecieve_PubSub_Test()
        {
            Action<bool> test = simulateIsolatedProcesses =>
            {
                int MESSAGES_COUNT = 10000;
                int MAX_TEST_TIME_MS = 15 * 1000;

                var sender_factory = simulateIsolatedProcesses ? new MonQueFactory() : _factory;
                var sendQueue = sender_factory.GetMessagesPublisher<TestMessage>();
                //var rec = Configuration.GetQueue<TestMessage>();
               
                int count = 0;
                TestMessage lastMsg = null;

                Action<TestMessage> listener = msg =>
                {
                    Interlocked.Increment(ref count);
                    lastMsg = msg;
                };
                var receiver_factory = simulateIsolatedProcesses ? new MonQueFactory() : _factory;
                receiver_factory.RegisterEventListener<TestMessage>(listener);
                

                Thread.Sleep(300);// let the receiver start listening in pubsub. In WQ this is not needed

                DateTime startTime = DateTime.UtcNow;
                Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
                    .ForEach(m => sendQueue.Send(m, QueueMode.PubSub));

                while (count < MESSAGES_COUNT && DateTime.UtcNow - startTime < TimeSpan.FromMilliseconds(MAX_TEST_TIME_MS))
                {
                    Thread.Sleep(500);
                }
                Assert.AreEqual(MESSAGES_COUNT, count, "isolated=" + simulateIsolatedProcesses);
                //Assert.AreEqual(MESSAGES_COUNT, lastMsg.IntVal, "IsolationMode=" + isolated);
            };

            test(true);
            test(false);
        }

        [TestMethod]
        public void WorkQueue_SimpleSendReceiveTest()
        {
           
            int MESSAGES_COUNT = 10000;
            int MAX_TEST_TIME_MS = 15 * 1000;
            int count = 0;
            DateTime startTime = DateTime.UtcNow;

            // Start producing jobs (async)
            Task producer = Task.Factory.StartNew(() =>
            {
                var sender_factory = new MonQueFactory();
                var sendQueue = sender_factory.GetMessagesPublisher<TestMessage>();
                Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
                 .ForEach(m => sendQueue.Send(m, QueueMode.WorkQueue));

            });

            // start the job worker
            var receiver_factory = new MonQueFactory();
            receiver_factory.RegisterWorker<TestMessage>(msg => { Interlocked.Increment(ref count); });

            // wait till all items are done or timeout         
            while (count < MESSAGES_COUNT && DateTime.UtcNow - startTime < TimeSpan.FromMilliseconds(MAX_TEST_TIME_MS))
            {
                Thread.Sleep(500);
            }
            Assert.AreEqual(MESSAGES_COUNT, count);
        }

        //[TestMethod]
        //public void SimpleSendRecieve_WorkQueue_Test()
        //{
        //    SimpleSendReceiveHelper(QueueMode.WorkQueue);
        //}

        //private static void SimpleSendReceiveHelper(QueueMode mode)
        //{
        //    int MESSAGES_COUNT = 500;
        //    int MAX_TEST_TIME_MS = 10 * 1000;
        //    var sendQueue = Configuration.GetQueue<TestMessage>();

        //    int count = 0;
        //    TestMessage lastMsg = null;
        //    Task t = new Task(() =>
        //    {
        //        var rec = Configuration.GetQueue<TestMessage>();
        //        do
        //        {
        //            if(mode == QueueMode.PubSub)
        //                lastMsg = rec.ReceiveEvent();
        //            else
        //                lastMsg = rec.ReceiveAction();

        //            //Console.WriteLine("processed " + lastMsg.StringVal);
        //            Interlocked.Increment(ref count);
        //        }
        //        while (count < MESSAGES_COUNT); // This reduces the 'strength' of the test (maybe the queue produces more items...) - but saves time. Recommended to change to while(true) and disable the time assertion from time to time
        //    });
        //    t.Start();

        //    if(mode== QueueMode.PubSub) Thread.Sleep(300);// let the receiver start listening in pubsub. In WQ this is not needed
        //    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
        //        .ForEach(m=> sendQueue.Send(m, mode));

        //    bool finished = t.Wait(MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages
        //    Assert.IsTrue(finished, "Test did not finish due to timeout. Handled so far: " + count);
        //    Assert.AreEqual(MESSAGES_COUNT, count);
        //    Assert.AreEqual(MESSAGES_COUNT, lastMsg.IntVal);
        //}




        [TestMethod]
        public void PubSubTest_OneProducer_N_consumers_each_should_receive_all_events()
        {
            Action<bool> test = simulateIsolatedProcesses =>
                {
                    int MESSAGES_COUNT = 20000;
                    int MAX_TEST_TIME_MS = 30 * 1000;
                    int CONSUMERS_COUNT = 15;

                    var sender_factory = simulateIsolatedProcesses ? new MonQueFactory() : _factory;
                    var producer = sender_factory.GetMessagesPublisher<TestMessage>();

                    int globalCounter = 0;
                    for (int i = 0; i < CONSUMERS_COUNT; i++)
                    {
                        var receiver_factory = simulateIsolatedProcesses ? new MonQueFactory() : _factory;
                        receiver_factory.RegisterEventListener<TestMessage>(msg => Interlocked.Increment(ref globalCounter));
                    }

                    // In PubSub scenario it is important to start the receivers before the producer - otherwise - messages are lost
                    Thread.Sleep(100);
                    DateTime startTime = DateTime.UtcNow;
                    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
                        .ForEach(m => producer.Send(m));

                    //bool finished = Task.WaitAll(tasks.ToArray(), MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages
                    while (globalCounter < MESSAGES_COUNT * CONSUMERS_COUNT && DateTime.UtcNow - startTime < TimeSpan.FromMilliseconds(MAX_TEST_TIME_MS))
                    {
                        Thread.Sleep(500);
                    }

                    Assert.AreEqual(MESSAGES_COUNT * CONSUMERS_COUNT, globalCounter, "isolated=" + simulateIsolatedProcesses);
                };
            test(true); // each consumer represents an isolated process
            test(false);// many consumers in a single process. This should cause only a single connection to the mongodb
        }


        [TestMethod]
        public void WorkQueueTest_OneProducer_N_consumers_should_shareTheLoad()
        {
            int MESSAGES_COUNT = 500;
            int MAX_TEST_TIME_MS = 30 * 1000;
            int CONSUMERS_COUNT = 100;

            var sender_factory = new MonQueFactory() ;
            var producer = sender_factory.GetMessagesPublisher<TestMessage>();

            int globalCounter = 0;
            for (int i = 0; i < CONSUMERS_COUNT; i++)
            {
                var receiver_factory = new MonQueFactory() ; // each factory represents a different process communicating with mongo. Assumption is that no 2 workers on same message type on the same process
                int local = i;
                Action<TestMessage> workerAction = msg=>
                    {
                        Interlocked.Increment(ref globalCounter);
                        Console.WriteLine("Consumer:{0} Msg:{1}", local, msg.IntVal);
                        Thread.Sleep(100);
                    };

                receiver_factory.RegisterWorker<TestMessage>(workerAction);
            }
         
            // start shooting some jobs...
            DateTime startTime = DateTime.UtcNow;
            Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
                .ForEach(m => producer.Send(m));

            while (globalCounter < MESSAGES_COUNT  && DateTime.UtcNow - startTime < TimeSpan.FromMilliseconds(MAX_TEST_TIME_MS))
            {
                Thread.Sleep(500);
            }

            Thread.Sleep(1000); // makes this test stronger - in case there are still workers doing stuff and taking the same jobs. We should consider removing the while and limit this test only by time
            Assert.AreEqual(MESSAGES_COUNT , globalCounter);
        }


        //[TestMethod]
        //public void WorkQueueTest_OneProducer_N_workers_should_share_the_jobs()
        //{
        //    int MESSAGES_COUNT = 1000;
        //    int MAX_TEST_TIME_MS = 30 * 1000;
        //    int CONSUMERS_COUNT = 30;

        //    var producer = Configuration.GetQueue<TestMessage>();

        //    int globalCounter = 0;
        //    Action consumer = () =>
        //    {
        //        string name = new Random(10000).NextDouble().ToString();
        //        TestMessage lastMsg;
        //        var queue = Configuration.GetQueue<TestMessage>();
        //        do
        //        {
        //            lastMsg = queue.ReceiveAction();
        //            //Console.WriteLine(lastMsg.StringVal);
        //            Interlocked.Increment(ref globalCounter);
        //        }
        //        while (true);
        //        //while (lastMsg.IntVal < MESSAGES_COUNT);
        //    };

        //    var tasks = Enumerable.Range(1, CONSUMERS_COUNT).Select(i =>
        //        Task.Factory.StartNew(consumer, TaskCreationOptions.LongRunning)).ToList();

        //    // In PubSub scenario it is important to start the receivers before the producer - otherwise - messages are lost
        //    Thread.Sleep(100);
        //    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
        //        .ForEach(m => producer.Send(m, QueueMode.WorkQueue));

        //    bool finished = Task.WaitAll(tasks.ToArray(), MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages

        //    //Assert.IsTrue(finished, "Test did not finish due to timeout. Finished Sofar: " + globalCounter);
        //    Assert.AreEqual(MESSAGES_COUNT, globalCounter);
        //}


        //[TestMethod]
        //public void WorkQueueTest_OneProducer_N_workers_ProducerStartFirst_shouldGetAllMessages()
        //{
        //    int MESSAGES_COUNT = 10000;
        //    int MAX_TEST_TIME_MS = 10 * 1000;
        //    int CONSUMERS_COUNT = 30;

        //    var producer = Configuration.GetQueue<TestMessage>();
          
          

        //    int globalCounter = 0;
        //    Action consumer = () =>
        //    {
        //        string name = new Random(10000).NextDouble().ToString();
        //        TestMessage lastMsg;
        //        var queue = Configuration.GetQueue<TestMessage>();
        //        do
        //        {
        //            lastMsg = queue.ReceiveAction();
        //            //Console.WriteLine(lastMsg.StringVal);
        //            Interlocked.Increment(ref globalCounter);
        //        }
        //        while (true);
        //        //while (lastMsg.IntVal < MESSAGES_COUNT);
        //    };

        //    //start producer
        //    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
        //    .ForEach(m => producer.Send(m, QueueMode.WorkQueue));

        //    // start workers
        //    var tasks = Enumerable.Range(1, CONSUMERS_COUNT).Select(i =>
        //    Task.Factory.StartNew(consumer, TaskCreationOptions.LongRunning)).ToList();


        //    // Add more work after the workers are working
        //    Thread.Sleep(300);
        //    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
        //        .ForEach(m => producer.Send(m, QueueMode.WorkQueue));


        //    bool finished = Task.WaitAll(tasks.ToArray(), MAX_TEST_TIME_MS); 

        //    //Assert.IsTrue(finished, "Test did not finish due to timeout. Finished Sofar: " + globalCounter);
        //    Assert.AreEqual(MESSAGES_COUNT * 2, globalCounter);
        //}


        [TestMethod]
        public void PubSub_N_Producer_M_consumers_each_should_receive_all_events()
        {
            int MESSAGES_COUNT = 1000;
            int MAX_TEST_TIME_MS = 10 * 1000;
            int CONSUMERS_COUNT = 9;
            int PRODUCERS_COUNT = 6;

            int globalCounter = 0;

           
            // Start consumers
            for (int i = 0; i < CONSUMERS_COUNT; i++)
            {
                _factory.RegisterEventListener<TestMessage>(msg =>
                {
                    Interlocked.Increment(ref globalCounter);
                });
            }
            // Start producers
            // In PubSub scenario it is important to start the receivers before the producer - otherwise - messages will go to dev/null
            Thread.Sleep(100);
            Action producer = () =>
            {
                var sendingQueue = _factory.GetMessagesPublisher<TestMessage>();
                Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList()
                    .ForEach(m => sendingQueue.Send(m));
            };

           
            var producerTasks = Enumerable.Range(1, PRODUCERS_COUNT).Select(i =>
               Task.Factory.StartNew(producer, TaskCreationOptions.LongRunning)).ToList();

            Thread.Sleep(MAX_TEST_TIME_MS);
            Assert.AreEqual(MESSAGES_COUNT * CONSUMERS_COUNT * PRODUCERS_COUNT, globalCounter);
        }



        private void DropCollection(string name)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["mongo-queue"].ConnectionString;
            var db = MongoDatabase.Create(connectionString);

            if (db.CollectionExists(name))
            {
                db.DropCollection(name);
            }
        }
    }



    public class TestMessage
    {
        public TestMessage() { }
        public TestMessage(int intVal, string stringVal) { IntVal = intVal; StringVal = stringVal; }

        
        public int IntVal { get; set; }
        public string StringVal { get; set; }
    }


    //public static class Configuration
    //{
    //    public static MongoQueue<T> GetQueue<T>() where T : class
    //    {
    //        var connectionString = ConfigurationManager.ConnectionStrings["mongo-queue"].ConnectionString;
    //        var queueSize = long.Parse(ConfigurationManager.AppSettings["mongo-queue.size"]);

    //        return new MongoQueue<T>(connectionString, queueSize);
    //    }
    //}
}
