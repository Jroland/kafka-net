using System;
using System.Linq;
using System.Text;

namespace KafkaNet.Common
{
    /// <summary>
    /// Provides Big Endian conversion extensions to required types for the Kafka protocol.
    /// </summary>
    public static class Extensions
    {
        public static byte[] ToIntSizedBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            return value.Length.ToBytes()
                        .Concat(value.ToBytes())
                        .ToArray();
        }

        public static byte[] ToInt16SizedBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            return ((Int16)value.Length).ToBytes()
                        .Concat(value.ToBytes())
                        .ToArray();
        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();
            
            //UTF8 is array of bytes, no endianness
            return Encoding.Default.GetBytes(value);
        }

        public static byte[] ToBytes(this Int16 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this Int32 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this Int64 value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this float value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this double value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this char value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

        public static byte[] ToBytes(this bool value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }
        
        public static Int32 ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(value.Reverse().ToArray(), 0);
        }
    }
}
