kafka-net Release Notes
=========

Version 0.9.0.14
-------
Fix memory leak in NagleBlockingCollection.
Timeout does not reset when new data is added.
Fix thread contention when producer has many threads loading data into it's buffer.
Fix many deadlock senarios on cancelling and disposing.
More unit tests around threading.


Version 0.9.0.1
-------

#### Feature: Nagle Producer
The producer class has been significantly updated to use a message batching technique similar to the [nagle algorithm].  


The producer accepts messages and groups them together into a single batched transmission.  The total number of messages to batch before sending and the maximum amount of time to wait for the max batch size, is configurable.  Tunning these two parameters, along with gzip compression can increase the driver throughput by orders of magnitude.  

```sh
var producer = new Producer(new BrokerRouter(options)) 
    { 
        BatchSize = 100,
        BatchDelayTime = TimeSpan.FromMilliseconds(100)
    };

// BatchSize - The producer will wait until it receives 100 messages, group them together into one request and send.
// BatchDelayTime - If the producer has not received 100 messages within 100 milliseconds, the producer will send what it has received.

```


#### Feature: Memory management
The producer now has better options for managing how much memory it consumes when it starts to get backed up.  

There are now two parameters on the producer constructor:
MaximumAsyncRequests
MaximumMessageBuffer

These two parameters prevents the producer from going over a maximum of resources used in terms of network and memory.

##### MaximumMessageBuffer
This parameter represents the maximum number of messages the producer will hold in its buffer at any one time.  This includes all in flight messages and those buffered by the batching mechanism.  This maximum will be hit if more messages arrive to the producer than it can send to Kafka.  When the maximum is reached, the producer will block on any new messages until space is available.

##### MaximumAsyncRequests 
This parameter represents the maximum number of queued up async TCP commands allowed in flight at any one time.  This can occur when the batch size is too low and the producer creates a high number of transmission requests out to the Kafka brokers.  Having thousands of queued up async messages can adversly affect memory and increase timeout errors.

```sh
var producer = new Producer(new BrokerRouter(options), maximumAsyncRequests: 30, maximumMessageBuffer:1000);

//maximum outbound async requests will be limited to 30
//maximum amount of messages in the producer at any one time will be limited to 1000
```

#### Issues/Features Summary
* Fix some integration tests to run on any Kafka configuration.  More need conversion.
* Redesign of TcpKafkaSockets 
    * Performance improvements
    * Remove several deadlock senarios
    * Remove several race conditions
* Nagle producer
    * Memory management
    * Significant performance improvement
* Add MaximumReconnectionTimeout 
    * Put a maximum amount of time to wait when backing off
* Update documentation in code
* Update/extend unit tests





[nagle algorithm]:http://en.wikipedia.org/wiki/Nagle%27s_algorithm