monQue
===========

Simple Worker Queue & PubSub implementation with MongoDB backend.

The project initially started as a fork of the nice work of Captain Codeman (http://captaincodeman.com/2011/05/28/simple-service-bus-message-queue-mongodb/)
However, it addresses different requirements, and this is why it is a separate project.

Project Goals:
- Provide simple WorkQueue implementation to distribute work to multiple workers
- Provide simple Pub-Sub implementation for event-driven, distributed  architectures
- Use MongoDB as the backend storage engine. 
This makes this project an interesting alternative to 'standard' implementations (Rabbit, AMQP,Redis, MSMQ?) for developers who already use MongoDB and do not wish to add another moving part to their environment.
- Library only. Unlike Resque, there is no additional process to monitor.
