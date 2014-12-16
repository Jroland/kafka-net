using System;
using System.IO;
using System.Linq;
using kafka_tests.Helpers;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolMessageTests
    {
        [Test]
        [ExpectedException(typeof(FailCrcCheckException))]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var testMessage = new Message(value: "kafka test message.", key: "test");

            var encoded = Message.EncodeMessage(testMessage);
            encoded[0] += 1;
            var result = Message.DecodeMessage(0, encoded).First();
        }

        [Test]
        [TestCase("test key", "test message")]
        [TestCase(null, "test message")]
        [TestCase("test key", null)]
        [TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message(key: key, value: value);

            var encoded = Message.EncodeMessage(testMessage);
            var result = Message.DecodeMessage(0, encoded).First();

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

            var result = Message.EncodeMessageSet(messages);

            Assert.That(expected, Is.EqualTo(result));
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            //This message set has a truncated message bytes at the end of it
            var result = Message.DecodeMessageSet(MessageHelper.FetchResponseMaxBytesOverflow).ToList();

            Assert.That(result.Count, Is.EqualTo(529));
        }

        [Test]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            // arrange
            var offset = (Int64)0;
            var message = new Byte[] { };
            var messageSize = message.Length + 1;
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);
            binaryWriter.Write(offset);
            binaryWriter.Write(messageSize);
            binaryWriter.Write(message);
            var payloadBytes = memoryStream.ToArray();

            // act/assert
            Assert.Throws<BufferUnderRunException>(() => Message.DecodeMessageSet(payloadBytes).ToList());
        }

        [Test]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new Byte[] { 1, 2, 3, 4 };
            var payload = CreatePayload(0, 0, 0, new Byte[] { 0 }, expectedPayloadBytes);

            // act/assert
            var messages = Message.DecodeMessageSet(payload).ToList();
            var actualPayload = messages.First().Value;

            // assert
            var expectedPayload = new Byte[] { 1, 2, 3, 4 };
            CollectionAssert.AreEqual(expectedPayload, actualPayload);
        }

        private static Byte[] CreatePayload(Int64 offset, Byte magicByte, Byte attributes, Byte[] key, Byte[] payload)
        {
            var crcContentStream = new MemoryStream();
            var crcContentWriter = new BigEndianBinaryWriter(crcContentStream);
            crcContentWriter.Write(magicByte);
            crcContentWriter.Write(attributes);
            crcContentWriter.Write((Int32)key.Length);
            crcContentWriter.Write(key);
            crcContentWriter.Write(payload.Length);
            crcContentWriter.Write(payload);
            var crcContentBytes = crcContentStream.ToArray();

            var crc = Crc32.Compute(crcContentBytes);

            var messageStream = new MemoryStream();
            var messageWriter = new BigEndianBinaryWriter(messageStream);
            messageWriter.Write(crc);
            messageWriter.Write(crcContentBytes);
            var messageBytes = messageStream.ToArray();

            var messageSetStream = new MemoryStream();
            var messageSetWriter = new BigEndianBinaryWriter(messageSetStream);
            var messageSize = messageBytes.Length;
            messageSetWriter.Write(offset);
            messageSetWriter.Write(messageSize);
            messageSetWriter.Write(messageBytes);
            return messageSetStream.ToArray();
        }
    }
}
