using System;
using MongoDB.Bson;

namespace monQue
{
	public class MongoMessage<T> where T : class {
		public MongoMessage (T message, QueueMode mtype = QueueMode.PubSub) {
			Enqueued = DateTime.UtcNow;
			MType = mtype;
			Message = message;
		}

		public ObjectId Id { get; private set; }

		public DateTime Enqueued { get; private set; }

		public DateTime Dequeued { get; set; }

		public QueueMode MType { get; set; }

		/// <summary>
		/// Actual messager payload
		/// </summary>
		public T Message { get; private set; }
	}
}