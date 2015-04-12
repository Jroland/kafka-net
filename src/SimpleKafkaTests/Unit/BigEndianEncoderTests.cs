using System;
using System.IO;
using SimpleKafka.Common;
using NUnit.Framework;
using SimpleKafka;

namespace SimpleKafkaTests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    [TestFixture]
    [Category("Unit")]
    public class BigEndianEncoderTests
    {
        [Theory]
        [TestCase((Int64)0, new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int64)1, new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int64)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int64.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(Int64.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void Int64Tests(Int64 number, Byte[] expectedBytes)
        {
            var buffer = new byte[8];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(number);
            Assert.That(buffer, Is.EqualTo(expectedBytes));
        }
        [Theory]
        [TestCase((UInt32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((UInt32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase(UInt32.MinValue, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(UInt32.MaxValue, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(UInt32 number, Byte[] expectedBytes)
        {
            var buffer = new byte[4];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(number);
            Assert.That(buffer, Is.EqualTo(expectedBytes));
        }

        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(Int32 number, Byte[] expectedBytes)
        {
            var buffer = new byte[4];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(number);
            Assert.That(buffer, Is.EqualTo(expectedBytes));
        }

        [Theory]
        [TestCase((Int16)0, new Byte[] { 0x00, 0x00 })]
        [TestCase((Int16)1, new Byte[] { 0x00, 0x01 })]
        [TestCase((Int16)(-1), new Byte[] { 0xFF, 0xFF })]
        [TestCase(Int16.MinValue, new Byte[] { 0x80, 0x00 })]
        [TestCase(Int16.MaxValue, new Byte[] { 0x7F, 0xFF })]
        public void Int16Tests(Int16 number, Byte[] expectedBytes)
        {
            var buffer = new byte[2];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(number);
            Assert.That(buffer, Is.EqualTo(expectedBytes));
        }


        [Theory]
        [TestCase("0000", new Byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.Int16)]
        [TestCase("0000", new Byte[] { 0x00, 0x00, 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.Int32)]
        [TestCase("0000", new Byte[] { 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.None)]
        [TestCase("€€€€", new Byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.Int16)]
        [TestCase("€€€€", new Byte[] { 0x00, 0x00, 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.Int32)]
        [TestCase("€€€€", new Byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.None)]
        [TestCase("", new Byte[] { }, StringPrefixEncoding.None)]
        [TestCase("", new Byte[] { 0x00, 0x00 }, StringPrefixEncoding.Int16)]
        [TestCase("", new Byte[] { 0x00, 0x00, 0x00, 0x00 }, StringPrefixEncoding.Int32)]
        [TestCase(null, new Byte[] { }, StringPrefixEncoding.None)]
        [TestCase(null, new Byte[] { 0xFF, 0xFF }, StringPrefixEncoding.Int16)]
        [TestCase(null, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, StringPrefixEncoding.Int32)]
        public void StringTests(String value, Byte[] expectedBytes, StringPrefixEncoding encoding)
        {
            var buffer = new byte[expectedBytes.Length];
            var encoder = new KafkaEncoder(buffer);
            encoder.Write(value, encoding);
            Assert.That(encoder.Offset, Is.EqualTo(expectedBytes.Length));
            Assert.That(buffer, Is.EqualTo(expectedBytes));
        }

    }
}
