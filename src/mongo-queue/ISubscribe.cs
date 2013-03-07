namespace monQue
{
    public interface ISubscribe<out T> where T : class
    {
        // Receive a PubSub or a WorkQueue message. Will not mark the message as delivered, other recipients will receive it as well.
        T ReceiveEvent();
        
        //Receive a WorkQueue message and marks it as delivered. No other subscriber will get that message
        T ReceiveAction();
    }
}