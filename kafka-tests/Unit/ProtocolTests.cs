using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka;
using Kafka.Model;
using NUnit.Framework;
using kafka_net.Common;

namespace kafka_tests
{
    [TestFixture]
    public class ProtocolTests
    {
        [Test]
        public void EnsureHeaderShouldPackCorrectByteLengths()
        {
            var protocol = new Protocol();
            var result = protocol.EncodeHeader(new FetchRequest {ClientId = "test", CorrelationId = 123456789});

            Assert.That(result.Length, Is.EqualTo(14));
            Assert.That(result, Is.EqualTo(new byte[] { 0, 1, 0, 0, 7, 91, 205, 21, 0, 4, 116, 101, 115, 116 }));
        }

        [Test]
        [ExpectedException(typeof(FailCrcCheckException))]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var protocol = new Protocol();
            var testMessage = new Message
            {
                Key = "test",
                Value = "kafka test message."
            };

            var encoded = protocol.EncodeMessage(testMessage);
            encoded[0] += 1;
            var result = protocol.DecodeMessage(0, encoded).First();
        }

        [Test]
        [TestCase("test key", "test message")]
        [TestCase(null, "test message")]
        [TestCase("test key", null)]
        [TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var protocol = new Protocol();
            var testMessage = new Message
                {
                    Key = key,
                    Value = value
                };

            var encoded = protocol.EncodeMessage(testMessage);
            var result = protocol.DecodeMessage(0, encoded).First();

            Assert.That(testMessage.Key, Is.EqualTo(result.Key));
            Assert.That(testMessage.Value, Is.EqualTo(result.Value));
        }
    }
}
