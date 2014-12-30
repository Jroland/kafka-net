using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Text;

namespace KafkaNet.Common
{
    /// <summary>
    /// A BinaryWriter that stores values in BigEndian format.
    /// </summary>
    /// <remarks>
    /// Booleans, bytes and byte arrays will be written directly.
    /// All other values will be converted to a byte array in BigEndian byte order and written.
    /// Characters and Strings will all be encoded in UTF-8 (which is byte order independent).
    /// </remarks>
    /// <remarks>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// The code was modified to implement Kafka specific byte handling.
    /// </remarks>
    public class BigEndianBinaryWriter : BinaryWriter
    {
        public BigEndianBinaryWriter(Stream stream)
            : base(stream, Encoding.UTF8)
        {
            Contract.Requires(stream != null);
        }

        public BigEndianBinaryWriter(Stream stream, Boolean leaveOpen)
            : base(stream, Encoding.UTF8, leaveOpen)
        {
            Contract.Requires(stream != null);
        }

        public override void Write(Decimal value)
        {
            var ints = Decimal.GetBits(value);
            Contract.Assume(ints != null);
            Contract.Assume(ints.Length == 4);

            if (BitConverter.IsLittleEndian)
                Array.Reverse(ints);

            for (var i = 0; i < 4; ++i)
            {
                var bytes = BitConverter.GetBytes(ints[i]);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(bytes);

                Write(bytes);
            }
        }

        public override void Write(Single value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(Double value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(Int16 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(Int32 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(Int64 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(UInt16 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(UInt32 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(UInt64 value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(string value)
        {
            throw new NotSupportedException("Kafka requires specific string length prefix encoding.");
        }

        public void Write(byte[] value, StringPrefixEncoding encoding)
        {
            if (value == null)
            {
                Write(-1);
                return;
            }

            if (encoding == StringPrefixEncoding.Int16)
                Write((Int16)value.Length);
            else
                Write(value.Length);

            Write(value);
        }

        public void Write(string value, StringPrefixEncoding encoding)
        {
            if (string.IsNullOrEmpty(value))
            {
                Write(-1);
                return;
            }

            if (encoding == StringPrefixEncoding.Int16)
                Write((Int16)value.Length);
            else
                Write(value.Length);

            Write(Encoding.UTF8.GetBytes(value));
        }


        private void WriteBigEndian(Byte[] bytes)
        {
            Contract.Requires(bytes != null);

            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);

            Write(bytes);
        }
    }

    public enum StringPrefixEncoding
    {
        Int16,
        Int32
    };

    public class KafkaMessagePacker
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;

        public KafkaMessagePacker()
        {
            _stream = new BigEndianBinaryWriter(new MemoryStream());
            Pack(IntegerByteSize); //pre-allocate space for buffer length
        }

        public KafkaMessagePacker Pack(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public KafkaMessagePacker Pack(Int32 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int16 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int64 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(byte[] buffer, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(buffer, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(data, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(List<string> data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            foreach (var item in data)
            {
                _stream.Write(item, encoding);
            }
            
            return this;
        }

        public byte[] Payload()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Pack((Int32)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);
            return buffer;
        }

        public byte[] PayloadNoLength()
        {
            var payloadLength = _stream.BaseStream.Length - IntegerByteSize;
            var buffer = new byte[payloadLength];
            _stream.BaseStream.Position = IntegerByteSize;
            _stream.BaseStream.Read(buffer, 0, (int)payloadLength);
            return buffer;
        }

        public byte[] CrcPayload()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            
            //copy the payload over
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, buffer.Length);
            buffer[0] = crc[0];
            buffer[1] = crc[1];
            buffer[2] = crc[2];
            buffer[3] = crc[3];
            
            return buffer;
        }
    }
}
