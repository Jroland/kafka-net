using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    internal static class Extensions
    {
        public static async Task ReadFullyAsync(this Stream stream, byte[] buffer, int offset, int numberOfBytes, CancellationToken token)
        {
            while (numberOfBytes > 0)
            {
                var bytesRead = await stream.ReadAsync(buffer, offset, numberOfBytes, token).ConfigureAwait(false);
                if (bytesRead <= 0)
                {
                    throw new EndOfStreamException();
                }
                numberOfBytes -= bytesRead;
                offset += bytesRead;
            }
        }

        public static TValue GetOrCreate<TKey,TValue>(this IDictionary<TKey,TValue> map, TKey key)
            where TValue : new()
        {
            TValue result;
            if (!map.TryGetValue(key, out result))
            {
                result = new TValue();
                map.Add(key, result);
            }
            return result;
        }

        public static TValue TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> map, TKey key)
        where TValue : class
        {
            TValue result;
            if (map.TryGetValue(key, out result))
            {
                return result;
            }
            else
            {
                return null;
            }
        }

        public static TValue GetOrCreate<TKey, TValue>(this IDictionary<TKey, TValue> map, TKey key, Func<TValue> creator)
        {
            TValue result;
            if (map.TryGetValue(key, out result))
            {
                return result;
            }
            else
            {
                result = creator();
                map.Add(key, result);
                return result;
            }
        }
    }
}
