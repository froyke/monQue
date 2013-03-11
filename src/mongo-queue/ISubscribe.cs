using System;
namespace monQue
{
    interface ISubscribe<out T> where T : class
    {
        void ReceiveEvents(Action<T> handler);
    }

    interface IWorkerQueue<out T> where T : class
    {
        /// <summary>
        /// Receive a WorkQueue message and marks it as delivered. No other subscriber will get that message
        /// </summary>
        /// <param name="worker"></param>
        void RegisterWorker(Action<T> worker);
    }
}