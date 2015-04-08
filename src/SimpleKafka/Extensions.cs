using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    internal static class Extensions
    {
        public static TValue FindOrCreate<TKey,TValue>(this Dictionary<TKey,TValue> map, TKey key)
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

        public static TValue TryGetValue<TKey, TValue>(this Dictionary<TKey, TValue> map, TKey key)
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
    }
}
