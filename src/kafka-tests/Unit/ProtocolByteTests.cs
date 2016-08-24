using System;
using System.Collections.Generic;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    /// <summary>
    /// From http://kafka.apache.org/protocol.html#protocol_types
    /// The protocol is built out of the following primitive types.
    ///
    /// Fixed Width Primitives:
    /// int8, int16, int32, int64 - Signed integers with the given precision (in bits) stored in big endian order.
    ///
    /// Variable Length Primitives:
    /// bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content. 
    /// A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
    ///
    /// Arrays:
    /// This is a notation for handling repeated structures. These will always be encoded as an int32 size containing 
    /// the length N followed by N repetitions of the structure which can itself be made up of other primitive types. 
    /// In the BNF grammars below we will show an array of a structure foo as [foo].
    /// 
    /// Message formats are from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-CommonRequestandResponseStructure
    /// 
    /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
    ///  Size => int32    : The Size field gives the size of the subsequent request or response message in bytes. 
    ///                     The client can read requests by first reading this 4 byte size as an integer N, and 
    ///                     then reading and parsing the subsequent N bytes of the request.
    /// 
    /// Request Header => api_key api_version correlation_id client_id 
    ///  api_key => INT16             -- The id of the request type.
    ///  api_version => INT16         -- The version of the API.
    ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
    ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
    /// 
    /// Response Header => correlation_id 
    ///  correlation_id => INT32      -- The user-supplied value passed in with the request
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class ProtocolByteTests
    {
        /// <summary>
        /// Produce Request (Version: 0,1,2) => acks timeout [topic_data] 
        ///  acks => INT16                   -- The number of nodes that should replicate the produce before returning. -1 indicates the full ISR.
        ///  timeout => INT32                -- The time to await a response in ms.
        ///  topic_data => topic [data] 
        ///    topic => STRING
        ///    data => partition record_set 
        ///      partition => INT32
        ///      record_set => BYTES
        /// 
        /// where:
        /// record_set => MessageSetSize MessageSet
        ///  MessageSetSize => int32
        /// </summary>
        [Test]
        public void ProduceApiRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 1, 2, -1)] short acks, 
            [Values(0, 1, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(1, 2, 3)] int messagesPerSet)
        {
            var randomizer = new Randomizer();
            var clientId = nameof(ProduceApiRequest);

            var request = new ProduceRequest {
                Acks = acks,
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                TimeoutMS = timeoutMilliseconds,
                Payload = new List<Payload>(),
                ApiVersion = version
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new Payload {
                    Topic = topic + t,
                    Partition = t % totalPartitions,
                    Codec = MessageCodec.CodecNone,
                    Messages = new List<Message>()
                };
                for (var m = 0; m < messagesPerSet; m++) {
                    var message = new Message {
                        MagicNumber = 1,
                        Timestamp = DateTime.UtcNow,
                        Key = m > 0 ? new byte[8] : null,
                        Value = new byte[8*(m + 1)]
                    };
                    if (message.Key != null) {
                        randomizer.NextBytes(message.Key);
                    }
                    randomizer.NextBytes(message.Value);
                    payload.Messages.Add(message);
                }
                request.Payload.Add(payload);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                reader => {
                    reader.AssertRequestHeader(version, request);
                    reader.AssertProduceRequest(version, request);
                });
        }
    }
}