using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Text;

namespace KafkaNet.Common
{
    /// <summary>
    /// A BinaryReader that is BigEndian aware binary reader.
    /// </summary>
    /// <remarks>
    /// Booleans, bytes and byte arrays will be written directly.
    /// All other values will be converted to a byte array in BigEndian byte order and written.
    /// Characters and Strings will all be encoded in UTF-8 (which is byte order independent).
    /// </remarks>
    /// <remarks>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </remarks>
    public class BigEndianBinaryReader : BinaryReader
    {
        public BigEndianBinaryReader(Stream input)
            : base(input)
        {
            Contract.Requires(input != null);
        }

        public BigEndianBinaryReader(Stream input, Boolean leaveOpen)
            : base(input, Encoding.UTF8, leaveOpen)
        {
            Contract.Requires(input != null);
        }

        public override Decimal ReadDecimal()
        {
            var bytes = GetNextBytesNativeEndian(16);

            var ints = new Int32[4];
            ints[0] = (Int32)bytes[0] << 0
                | (Int32)bytes[1] << 8
                | (Int32)bytes[2] << 16
                | (Int32)bytes[3] << 24;
            ints[1] = (Int32)bytes[4] << 0
                | (Int32)bytes[5] << 8
                | (Int32)bytes[6] << 16
                | (Int32)bytes[7] << 24;
            ints[2] = (Int32)bytes[8] << 0
                | (Int32)bytes[9] << 8
                | (Int32)bytes[10] << 16
                | (Int32)bytes[11] << 24;
            ints[3] = (Int32)bytes[12] << 0
                | (Int32)bytes[13] << 8
                | (Int32)bytes[14] << 16
                | (Int32)bytes[15] << 24;

            return new Decimal(ints);
        }

        public override Single ReadSingle()
        {
            return Read(4, BitConverter.ToSingle);
        }

        public override Double ReadDouble()
        {
            return Read(8, BitConverter.ToDouble);
        }

        public override Int16 ReadInt16()
        {
            return Read(2, BitConverter.ToInt16);
        }

        public override Int32 ReadInt32()
        {
            return Read(4, BitConverter.ToInt32);
        }

        public override Int64 ReadInt64()
        {
            return Read(8, BitConverter.ToInt64);
        }

        public override UInt16 ReadUInt16()
        {
            return Read(2, BitConverter.ToUInt16);
        }

        public override UInt32 ReadUInt32()
        {
            return Read(4, BitConverter.ToUInt32);
        }

        public override UInt64 ReadUInt64()
        {
            return Read(8, BitConverter.ToUInt64);
        }

        private T Read<T>(Int32 size, Func<Byte[], Int32, T> converter) where T : struct
        {
            Contract.Requires(size >= 0);
            Contract.Requires(converter != null);

            var bytes = GetNextBytesNativeEndian(size);
            return converter(bytes, 0);
        }

        private Byte[] GetNextBytesNativeEndian(Int32 count)
        {
            Contract.Requires(count >= 0);
            Contract.Ensures(Contract.Result<Byte[]>() != null);
            Contract.Ensures(Contract.Result<Byte[]>().Length == count);

            var bytes = GetNextBytes(count);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        private Byte[] GetNextBytes(Int32 count)
        {
            Contract.Requires(count >= 0);
            Contract.Ensures(Contract.Result<Byte[]>() != null);
            Contract.Ensures(Contract.Result<Byte[]>().Length == count);

            var buffer = new Byte[count];
            var bytesRead = BaseStream.Read(buffer, 0, count);

            if (bytesRead != count)
                throw new EndOfStreamException();

            return buffer;
        }
    }
}
