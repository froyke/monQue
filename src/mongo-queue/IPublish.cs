namespace monQue
{
    public interface IPublish<in T> where T : class
    {
        /// <summary>
        /// Note that event listeners (pubsub mode) will be able to see 'Action' messages, But Workers can only receive (and acknowledge) 'Actions'
        /// </summary>
        /// <param name="message"></param>
        /// <param name="mode"></param>
        void Send(T message, QueueMode mode = QueueMode.PubSub);
    }

    public enum QueueMode
    {
        /// <summary>
        /// Multiple subscribers for the same message. No Delivery guaranty (if subscriber is offline - he'll never be able to get the messages)
        /// </summary>
        PubSub = 0,

        /// <summary>
        /// One subscriber for each message. When no workers around - messages will be buffered (until the limit of the queue size)
        /// </summary>
        WorkQueue = 1
    }
}