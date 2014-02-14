using System;
using System.Linq;
using System.Text;

namespace Kafka.Common
{
    public static class Extensions
    {
        public static byte[] ToBytes(this string value)
        {
            return Encoding.UTF8.GetBytes(value).Reverse().ToArray();
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
            return BitConverter.ToInt32(value, 0);
        }
    }
}
