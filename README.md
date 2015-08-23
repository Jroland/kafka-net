KafkaNetClient
=========

Native C# client for Apache Kafka.  

License
-----------
Copyright 2015, Gigya Inc under Apache License, V2.0. See LICENSE file.

Summary
-----------

This library is a fork of Jroland's [kafka-net](https://github.com/Jroland/kafka-net) library, with adjustments and improvements (not interchangeable with kafka-net, as there are breaking changes).

The original project is a .NET implementation of the [Apache Kafka] protocol.  The wire protocol portion is based on the [kafka-python] library writen by [David Arthur] and the general class layout attempts to follow a similar pattern as his project.  To that end, this project builds up from the low level KafkaConnection object for handling async requests to/from the kafka server, all the way up to a higher level Producer/Consumer classes.

##### Improvements and Changes:

- All the code is now async all the way and all blocking operations were removed (**except for the high-level `Consumer` class**).
- `ProtocolGateway`:
    * New class that allows simple handling of Kafka protocol messages with error recovery and metadata refreshes.
- `BrokerRouter`:
    * Breaking changes were made in order to make it async all-the-way.
    * Interface was changed to allow greater control working with broker metadata.
- `Producer`:
    * Was refactored to use `ProtocolGateway` when sending messages to Kafka (for better error recovery).
- `ManualConsumer`:
    * New class (uses `ProtocolGateway`) that allows simple fetches from Kafka brokers (in contrast to the high-level `Consumer` class that includes internal caching and other optimizations).
- Bug fixes
    * When sending messages to the same partition with the same Ack level, order is guaranteed.    





Examples
-----------
##### Producer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
using(var client = new Producer(router))
{
     await client.SendMessageAsync("TestTopic", new Message("hello world"));
}


```
##### Consumer
```sh
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));
var router = new BrokerRouter(options);
var consumer = new Consumer(new ConsumerOptions("TestHarness", new BrokerRouter(options)));

//Consume returns a blocking IEnumerable (ie: never ending stream)
foreach (var message in consumer.Consume())
{
    Console.WriteLine("Response: P{0},O{1} : {2}", 
        message.Meta.PartitionId, message.Meta.Offset, message.Value);  
}
```

##### TestHarness
The TestHarness project it a simple example console application that will read message from a kafka server and write them to the screen.  It will also take anything typed in the console and send this as a message to the kafka servers.  

Simply modify the kafka server Uri in the code to point to a functioning test server.


Pieces of the Puzzle
-----------
##### Protocol
The protocol has been divided up into concrete classes for each request/response pair.  Each class knows how to encode and decode itself into/from their appropriate Kafka protocol byte array.  One benefit of this is that it allows for a nice generic send method on the KafkaConnection.

##### KafkaConnection
Provides async methods on a persistent connection to a kafka broker (server).  The send method uses the TcpClient send async function and the read stream has a dedicated thread which uses the correlation Id to match send responses to the correct request.

##### BrokerRouter
Provides metadata based routing of messages to the correct Kafka partition.  This class also manages the multiple KafkaConnections for each Kafka server returned by the broker section in the metadata response.  Routing logic is provided by the IPartitionSelector.

##### ProtocolGateway
A convenience class that allows sending Kafka protocol messages easily, including error handling and metadata refresh on failure.

##### IPartitionSelector
Provides the logic for routing which partition the BrokerRouter should choose.  The default selector is the DefaultPartitionSelector which will use round robin partition selection if the key property on the message is null and a mod/hash of the key value if present.

##### Producer
Provides a higher level class which uses the combination of the BrokerRouter and KafkaConnection to send batches of messages to a Kafka broker.

##### Consumer
Provides a higher level class which will consumer messages from a whitelist of partitions from a single topic.  The consumption mechanism is a blocking IEnumerable of messages.  If no whitelist is provided then all partitions will be consumed creating one KafkaConnection for each partition leader.

#### ManualConsumer
A class which enables simple manual consuming of messages which encapsulates the Kafka protocol details and enables message fetching, offset fetching, and offset updating. All of this operations are on demand.

Status
-----------
Tested with Kafka 0.8.2.

This library is still work in progress and was still not deployed to production. We will update when it does.


##### The major items that needs work are:
* Better handling of options for providing customization of internal behaviour of the base API. (right now the classes pass around option parameters)
* General structure of the classes is not finalized and breaking changes will occur.
* Only Gzip compression is implemented, snappy on the todo.
* Currently only works with .NET Framework 4.5 as it uses the await command.
* Test coverage.
* Documentation.





[kafka-python]:https://github.com/mumrah/kafka-python
[Apache Kafka]:http://kafka.apache.org
[David Arthur]:https://github.com/mumrah