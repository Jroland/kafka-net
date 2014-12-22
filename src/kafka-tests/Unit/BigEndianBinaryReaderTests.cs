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
    public class BigEndianBinaryReaderTests
    {
        // validates my assumptions about the default implementation doing the opposite of this implementation
        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(Int32 expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BinaryReader(new MemoryStream(givenBytes));

            // act
            var actualValue = binaryReader.ReadInt32();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Theory]
        [TestCase((Int32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((Int32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((Int32)(-1), new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(Int32.MinValue, new Byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(Int32.MaxValue, new Byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(Int32 expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadInt32();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Theory]
        [TestCase((UInt32)0, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((UInt32)1, new Byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((UInt32)123456789, new Byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [TestCase(UInt32.MinValue, new Byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(UInt32.MaxValue, new Byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(UInt32 expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadUInt32();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
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
        public void SingleTests(Single expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadSingle();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
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
        public void DoubleTests(Double expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadDouble();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Theory]
        [TestCase("0000", new Byte[] { 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("€€€€", new Byte[] { 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        public void StringTests(String expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadString();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Theory]
        [TestCase('0', new Byte[] { 0x30 })]
        [TestCase('€', new Byte[] { 0xE2, 0x82, 0xAC })]
        public void CharTests(Char expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadChar();

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

        [Theory]
        [TestCase(new Char[] { '0', '0', '0', '0' }, new Byte[] { 0x30, 0x30, 0x30, 0x30 })]
        [TestCase(new Char[] { '€', '€', '€', '€' }, new Byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        public void CharArrayTests(Char[] expectedValue, Byte[] givenBytes)
        {
            // arrange
            var binaryReader = new BigEndianBinaryReader(givenBytes);

            // act
            var actualValue = binaryReader.ReadChars(givenBytes.Length);

            // assert
            Assert.That(expectedValue, Is.EqualTo(actualValue));
        }

    }
}
