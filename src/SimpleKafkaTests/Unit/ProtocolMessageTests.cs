using System;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SimpleKafka.Protocol;
using SimpleKafkaTests.Helpers;
using SimpleKafka.Common;
using SimpleKafka;

namespace SimpleKafkaTests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolMessageTests
    {
        [Test]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            Assert.Throws(Is.TypeOf<FailCrcCheckException>(), () =>
            {
                var testMessage = new Message(value: "kafka test message.", key: "test");
                var buffer = new byte[1024];
                var encoder = new KafkaEncoder(buffer);

                Message.EncodeMessage(testMessage, ref encoder);
                buffer[0] += 1;

                var decoder = new KafkaDecoder(buffer, 0, encoder.Offset);
                var result = Message.DecodeMessage(0, 0, ref decoder, encoder.Offset);
            });
        }

        [Test]
        [TestCase("test key", "test message")]
        [TestCase(null, "test message")]
        [TestCase("test key", null)]
        [TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message(key: key, value: value);

            var buffer = new byte[1024];
            var encoder = new KafkaEncoder(buffer);
            Message.EncodeMessage(testMessage, ref encoder);

            var decoder = new KafkaDecoder(buffer);
            var result = Message.DecodeMessage(0, 0, ref decoder, encoder.Offset);

            Assert.That(testMessage.Key, Is.EqualTo(result.Key));
            Assert.That(testMessage.Value, Is.EqualTo(result.Value));
        }

        [Test]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[]
                {
                    new Message("0", "1"),
                    new Message("1", "1"),
                    new Message("2", "1")
                };

            var buffer = new byte[expected.Length];
            var encoder = new KafkaEncoder(buffer);
            Message.EncodeMessageSet(ref encoder, messages);

            Assert.That(buffer, Is.EqualTo(expected));
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            //This message set has a truncated message bytes at the end of it
            var decoder = new KafkaDecoder(MessageHelper.FetchResponseMaxBytesOverflow);
            var result = Message.DecodeMessageSet(0, ref decoder, decoder.Length);

            var message = Encoding.UTF8.GetString(result.First().Value);
            
            Assert.That(message, Is.EqualTo("test"));
            Assert.That(result.Count, Is.EqualTo(529));
        }

        [Test]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            Assert.Throws<BufferUnderRunException>(() =>
            {
                // arrange
                var offset = (Int64)0;
                var message = new Byte[] { };
                var messageSize = 5;
                var payloadBytes = new byte[16];
                var encoder = new KafkaEncoder(payloadBytes);
                encoder.Write(offset);
                encoder.Write(messageSize);
                encoder.Write(message);

                var decoder = new KafkaDecoder(payloadBytes);

                Message.DecodeMessageSet(0, ref decoder, payloadBytes.Length);
            });
        }

        [Test]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new Byte[] { 1, 2, 3, 4 };
            var payload = MessageHelper.CreateMessage(0, new Byte[] { 0 }, expectedPayloadBytes);

            // act/assert
            var decoder = new KafkaDecoder(payload, 0, payload.Length);
            var messages = Message.DecodeMessageSet(0, ref decoder, payload.Length);
            var actualPayload = messages.First().Value;

            // assert
            var expectedPayload = new Byte[] { 1, 2, 3, 4 };
            CollectionAssert.AreEqual(expectedPayload, actualPayload);
        }
    }
}
