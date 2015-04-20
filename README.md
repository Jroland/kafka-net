simple-kafka-net
=========

Native C# client for Apache Kafka derived from [jroland/kafka-net].  

License
-----------
Original kafka-net Copyright 2014, James Roland
Modified version Copyright 2015, Nick Randell

Apache License, V2.0. See LICENSE file.

Summary
-----------
This project is a .NET implementation of the [Apache Kafka] protocol. The wire protocol portion is based on the [kafka-python] library writen by [David Arthur] and the general class layout attempts to follow a similar pattern as his project.  

It is very much work in progress but is designed to be asynchronous from the ground up, providing thin wrapper over the protocol and building up a robust broker manager with simple producer and consumer classes. Even though it is asynchronous, no threads are involved unless the runtime uses them. This does mean that it is not yet possible to have multiple messages in flight at the same time, but that will change.

The protocol encoding and decoding has also been modified to work as efficiently as possible without copying data around.

One of the aims of this fork is to allow the client to have much more control over the partitions to consume as this allows larger solutions to scale by having consumers running on different servers.

Testing makes use of docker to spin up test clusters to give control over different scenarios.

The current version 0.1 is not very robust, but generally works for single brokers ok.


Examples
-----------
##### Producer
```using (var broker = new KafkaBroker(new Uri("http://SERVER1:9092")))
{
    var producer = KafkaProducer.Create(brokers, new StringSerializer());
    await producer.SendAsync(KeyedMessage.Create("Test Topic", "Test"), CancellationToken.None);
}
```


##### Consumer
```using (var broker = new KafkaBroker(new Uri("http://SERVER1:9092")))
{
    var consumer = KafkaConsumer.Create(topic, brokers, new StringSerializer(),
        new TopicSelector { Topic = "Test Topic", Partition = 0, Offset = 0 });
    var result = await consumer.ReceiveAsync(CancellationToken.None);
    foreach (var message in result) 
    {
        Console.WriteLine("Received {0}", message.Value);
    }
}
```

The topic selector is used to determine which topics and offsets to use.

Things left to do
------------

- [ ] Make use of correlation id to allow multiple messages in flight at the same time
- [ ] Compression

Pieces of the Puzzle
-----------
##### Protocol
The protocol has been divided up into concrete classes for each request/response pair.  Each class knows how to encode and decode itself into/from their appropriate Kafka protocol byte array.  One benefit of this is that it allows for a nice generic send method on the KafkaConnection.

This has been kept almost identical to the original version, but the encoding/decoding is now done with a preallocated buffer.

##### KafkaConnection
Provides async methods on a persistent connection to a kafka broker (server).  Sending a message and receiving a response is carried out within a lock as only one message is in flight at any time. This will change!!

##### KafkaBrokers
Provides management of a group of brokers, maintaining a connection to each of the valid brokers

##### Producer
Provides a higher level class which uses the combination of the KafkaBrokers and KafkaConnection to send messages. There is no queuing or batching of messages internally. That would be the work of a higher level producer.

##### Consumer
Provides the ability to receive messages from brokers.






[kafka-python]:https://github.com/mumrah/kafka-python
[Apache Kafka]:http://kafka.apache.org
[David Arthur]:https://github.com/mumrah
[jroland/kafka-net]:https://github.com/jroland/kafka-net