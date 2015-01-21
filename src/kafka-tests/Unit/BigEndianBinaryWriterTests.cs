using System;
using System.IO;
using KafkaNet.Common;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    [TestFixture]
    [Category("Unit")]
    public class BigEndianBinaryWriterTests
    {
        // validates my assumptions about the default implementation doing the opposite of this implementation
        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(Int32 number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(Int32 number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase((Int16)0, new Byte[] { 0x00, 0x00 })]
        [TestCase((Int16)1, new Byte[] { 0x00, 0x01 })]
        [TestCase((Int16)(-1), new Byte[] { 0xFF, 0xFF })]
        [TestCase(Int16.MinValue, new Byte[] { 0x80, 0x00 })]
        [TestCase(Int16.MaxValue, new Byte[] { 0x7F, 0xFF })]
        public void Int16Tests(Int16 number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase((UInt32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((UInt32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((UInt32)123456789, new Byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [TestCase(UInt32.MinValue, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(UInt32.MaxValue, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(UInt32 number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase((Single)(0), new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Single)(1), new Byte[] { 0x3F, 0x80, 0x00, 0x00 })]
        [TestCase((Single)(-1), new Byte[] { 0xBF, 0x80, 0x00, 0x00 })]
        [TestCase(Single.MinValue, new Byte[] { 0xFF, 0x7F, 0xFF, 0xFF })]
        [TestCase(Single.MaxValue, new Byte[] { 0x7F, 0x7F, 0xFF, 0xFF })]
        [TestCase(Single.PositiveInfinity, new Byte[] { 0x7F, 0x80, 0x00, 0x00 })]
        [TestCase(Single.NegativeInfinity, new Byte[] { 0xFF, 0x80, 0x00, 0x00 })]
        [TestCase(Single.NaN, new Byte[] { 0xFF, 0xC0, 0x00, 0x00 })]
        public void SingleTests(Single number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase((Double)(0), new Byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Double)(1), new Byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Double)(-1), new Byte[] { 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(Double.MinValue, new Byte[] { 0xFF, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Double.MaxValue, new Byte[] { 0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Double.PositiveInfinity, new Byte[] { 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(Double.NegativeInfinity, new Byte[] { 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(Double.NaN, new Byte[] { 0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        public void DoubleTests(Double number, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void StringNotSupportedTest()
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);
            binaryWriter.Write("test");
        }

        [Theory]
        [TestCase("0000", new Byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.Int16)]
        [TestCase("0000", new Byte[] { 0x00, 0x00, 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.Int32)]
        [TestCase("0000", new Byte[] { 0x30, 0x30, 0x30, 0x30 }, StringPrefixEncoding.None)]
        [TestCase("€€€€", new Byte[] { 0x00, 0x04, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.Int16)]
        [TestCase("€€€€", new Byte[] { 0x00, 0x00, 0x00, 0x04, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.Int32)]
        [TestCase("€€€€", new Byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC }, StringPrefixEncoding.None)]
        [TestCase("", new Byte[] { }, StringPrefixEncoding.None)]
        [TestCase("", new Byte[] { 0x00, 0x00 }, StringPrefixEncoding.Int16)]
        [TestCase("", new Byte[] { 0x00, 0x00, 0x00, 0x00 }, StringPrefixEncoding.Int32)]
        [TestCase(null, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, StringPrefixEncoding.None)]
        [TestCase(null, new Byte[] { 0xFF, 0xFF }, StringPrefixEncoding.Int16)]
        [TestCase(null, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, StringPrefixEncoding.Int32)]
        public void StringTests(String value, Byte[] expectedBytes, StringPrefixEncoding encoding)
        {

            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value, encoding);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase('0', new Byte[] { 0x30 })]
        [TestCase('€', new Byte[] { 0xE2, 0x82, 0xAC })]
        public void CharTests(Char value, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase(new Char[] { '0', '0', '0', '0' }, new Byte[] { 0x30, 0x30, 0x30, 0x30 })]
        [TestCase(new Char[] { '€', '€', '€', '€' }, new Byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        public void CharArrayTests(Char[] value, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Theory]
        [TestCase(new Char[] { '0', '1', '2', '3' }, 1, 2, new Byte[] { 0x31, 0x32 })]
        [TestCase(new Char[] { '€', '2', '€', '€' }, 1, 2, new Byte[] { 0x32, 0xE2, 0x82, 0xAC })]
        public void CharSubArrayTests(Char[] value, Int32 index, Int32 count, Byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value, index, count);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

    }
}
