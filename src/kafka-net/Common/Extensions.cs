using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

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

        public static byte[] ToInt32PrefixedBytes(this byte[] value)
        {
            if (value == null) return (-1).ToBytes();

            return value.Length.ToBytes()
                        .Concat(value)
                        .ToArray();
        }

        public static string ToUtf8String(this byte[] value)
        {
            if (value == null) return string.Empty;

            return Encoding.UTF8.GetString(value);
        }

        public static KafkaDataPayload ToPayload(this byte[] data)
        {
            return new KafkaDataPayload {Buffer = data};
        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
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

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task<T> WithCancellation<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();

            var cancelRegistration = cancellationToken.Register(source => ((TaskCompletionSource<bool>) source).TrySetResult(true), tcs);
            
            using (cancelRegistration)
            {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false))
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }

            return await task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task WithCancellation(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();

            var cancelRegistration = cancellationToken.Register(source => ((TaskCompletionSource<bool>)source).TrySetResult(true), tcs);

            using (cancelRegistration)
            {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false))
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }


        /// <summary>
        /// Returns true if <see cref="WaitHandle"/> before timeout expires./>
        /// </summary>
        /// <param name="handle">The handle whose signal triggers the task to be completed.</param>
        /// <param name="timeout">The timespan to wait before returning false</param>
        /// <returns>The task returns true if the handle is signaled before the timeout has expired.</returns>
        /// <remarks>
        /// Original code from: http://blog.nerdbank.net/2011/07/c-await-for-waithandle.html
        /// There is a (brief) time delay between when the handle is signaled and when the task is marked as completed.
        /// </remarks>
        public static Task<bool> WaitAsync(this WaitHandle handle, TimeSpan timeout)
        {
            Contract.Requires<ArgumentNullException>(handle != null);
            Contract.Ensures(Contract.Result<Task>() != null);

            var tcs = new TaskCompletionSource<bool>();
            var localVariableInitLock = new object();
            lock (localVariableInitLock)
            {
                RegisteredWaitHandle callbackHandle = null;
                callbackHandle = ThreadPool.RegisterWaitForSingleObject(
                    handle,
                    (state, timedOut) =>
                    {
                        tcs.TrySetResult(!timedOut);

                        // We take a lock here to make sure the outer method has completed setting the local variable callbackHandle.
                        lock (localVariableInitLock)
                        {
                            if (callbackHandle!= null) callbackHandle.Unregister(null);
                        }
                    },
                    state: null,
                    millisecondsTimeOutInterval: (long)timeout.TotalMilliseconds,
                    executeOnlyOnce: true);
            }

            return tcs.Task;
        }

        /// <summary>
        /// Mainly used for testing, allows waiting on a single task without throwing exceptions.
        /// </summary>
        public static void SafeWait(this Task source, TimeSpan timeout)
        {
            try
            {
                source.Wait(timeout);
            }
            catch
            {
                //ignore an exception that happens in this source
            }
        }

        /// <summary>
        /// Splits a collection into given batch sizes and returns as an enumerable of batches.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> collection, int batchSize)
        {
            var nextbatch = new List<T>(batchSize);
            foreach (T item in collection)
            {
                nextbatch.Add(item);
                if (nextbatch.Count == batchSize)
                {
                    yield return nextbatch;
                    nextbatch = new List<T>(batchSize);
                }
            }
            if (nextbatch.Count > 0)
                yield return nextbatch;
        }

        /// <summary>
        /// Extracts a concrete exception out of a Continue with result.
        /// </summary>
        public static Exception ExtractException(this Task task)
        {
            if (task.IsFaulted == false) return null;
            if (task.Exception != null)
                return task.Exception.Flatten();
            
            return new ApplicationException("Unknown exception occured.");
        }
    }
}
