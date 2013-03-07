using System;
using System.Threading;
using monQue;
using common;

namespace subscriber
{
    internal class Program
    {
        private static readonly ManualResetEvent Reset = new ManualResetEvent(false);
        private static long _lastRead;
        private static long _readCount;
        private static int reportedByQ = 0;
        private static Timer _timer;
        private static readonly object Sync = new object();

        private static void Main(string[] args)
        {
            Console.WriteLine("Subscriber");
            Console.WriteLine("Press 'R' to Run, 'P' to Pause, 'X' to Exit ...");

            _timer = new Timer(TickTock, null, 1000, 1000);

            var t = new Thread(Run);
            t.Start();

            bool running = true;

            

            while (running)
            {
                ConsoleKeyInfo keypress = Console.ReadKey(true);
                switch (keypress.Key)
                {
                    case ConsoleKey.X:
                        Reset.Reset();
                        running = false;
                        break;
                    case ConsoleKey.P:
                        Reset.Reset();
                        Console.WriteLine("Paused ...");
                        break;
                    case ConsoleKey.R:
                        Reset.Set();
                        Console.WriteLine("Running ...");
                        break;
                }
            }

            t.Abort();
        }

        public static void Run()
        {
            ISubscribe<ExampleMessage> queue = Configuration.GetQueue<ExampleMessage>();

            while (true)
            {
                Reset.WaitOne();
                ExampleMessage message = queue.ReceiveAction();
                Interlocked.Increment(ref _readCount);
                reportedByQ = ((MongoQueue<ExampleMessage>)queue).TotalReceived;
            }
        }

        public static void TickTock(object state)
        {
            lock (Sync)
            {
                Console.WriteLine("Received {0} (total as seen by client {1}) total as reported by Q {2}", _readCount - _lastRead, _readCount, reportedByQ);
                _lastRead = _readCount;
            }
        }
    }
}