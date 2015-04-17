using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaNet.Model;

namespace KafkaNet
{
    public interface IKafkaConnection : IDisposable
    {
        /// <summary>
        /// The unique endpoint location of this connection.
        /// </summary>
        KafkaEndpoint Endpoint { get; }

        /// <summary>
        /// Value indicating the read polling thread is still active.
        /// </summary>
        bool ReadPolling { get; }

        /// <summary>
        /// Send raw payload data up to the connected endpoint.
        /// </summary>
        /// <param name="payload">The raw data to send to the connected endpoint.</param>
        /// <returns>Task representing the future success or failure of query.</returns>
        Task SendAsync(KafkaDataPayload payload);

        /// <summary>
        /// Send a specific IKafkaRequest to the connected endpoint.
        /// </summary>
        /// <typeparam name="T">The type of the KafkaResponse expected from the request being sent.</typeparam>
        /// <param name="request">The KafkaRequest to send to the connected endpoint.</param>
        /// <returns>Task representing the future responses from the sent request.</returns>
        Task<List<T>> SendAsync<T>(IKafkaRequest<T> request);
    }
}
