using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka.Common
{
    public static class Extensions
    {
        public static string ToUtf8String(this byte[] value)
        {
            if (value == null) return string.Empty;

            return Encoding.UTF8.GetString(value);
        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(this int value)
        {
            return BitConverter.GetBytes(value).Reverse().ToArray();
        }

    }
}
