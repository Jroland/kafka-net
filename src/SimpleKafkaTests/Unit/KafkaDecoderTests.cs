using System;
using System.IO;
using SimpleKafka;
using SimpleKafka.Common;
using NUnit.Framework;

namespace SimpleKafkaTests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    [TestFixture]
    [Category("Unit")]
    public class KafkaDecoderTests
    {
        [Theory]
        [TestCase((Int64)0, new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int64)1, new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int64)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int64.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(Int64.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void Int64Tests(Int64 expectedValue, Byte[] givenBytes)
        {
            var decoder = new KafkaDecoder(givenBytes);
            var actualValue = decoder.ReadInt64();
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(Int32 expectedValue, Byte[] givenBytes)
        {
            var decoder = new KafkaDecoder(givenBytes);
            var actualValue = decoder.ReadInt32();
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }


        [Theory]
        [TestCase((Int16)0, new Byte[] { 0x00, 0x00 })]
        [TestCase((Int16)1, new Byte[] { 0x00, 0x01 })]
        [TestCase((Int16)(-1), new Byte[] { 0xFF, 0xFF })]
        [TestCase(Int16.MinValue, new Byte[] { 0x80, 0x00 })]
        [TestCase(Int16.MaxValue, new Byte[] { 0x7F, 0xFF })]
        public void Int16Tests(Int16 expectedValue, Byte[] givenBytes)
        {
            var decoder = new KafkaDecoder(givenBytes);
            var actualValue = decoder.ReadInt16();
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }


        [Theory]
        [TestCase("0000", new Byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("€€€€", new Byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        [TestCase("", new Byte[] { 0x00, 0x00 })]
        [TestCase(null, new Byte[] { 0xFF, 0xFF })]
        public void StringTests(String expectedValue, Byte[] givenBytes)
        {
            var decoder = new KafkaDecoder(givenBytes);
            var actualValue = decoder.ReadString();
            Assert.That(decoder.Offset, Is.EqualTo(givenBytes.Length));
            Assert.That(actualValue, Is.EqualTo(expectedValue));
        }

    }
}
