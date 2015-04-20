using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using System.Net.Sockets;
using SimpleKafka.Protocol;

namespace SimpleKafka
{
    public static class KafkaConnectionFactory
    {
        public static async Task<KafkaConnection> CreateSimpleKafkaConnectionAsync(Uri address)
        {
            return await CreateSimpleKafkaConnectionAsync(address, CancellationToken.None).ConfigureAwait(false);
        }

        public static async Task<KafkaConnection> CreateSimpleKafkaConnectionAsync(Uri address, CancellationToken token)
        {
            var ipAddress = await GetFirstAddressAsync(address.Host, token);
            var endpoint = new IPEndPoint(ipAddress, address.Port);
            var connection = await KafkaConnection.CreateAsync(endpoint, token).ConfigureAwait(false);
            return connection;
        }

        private static async Task<IPAddress> GetFirstAddressAsync(string hostname, CancellationToken token)
        {
            try
            {
                //lookup the IP address from the provided host name
                var addresses = await Dns.GetHostAddressesAsync(hostname);

                if (addresses.Length > 0)
                {
                    Array.ForEach(addresses, address => Log.Debug("Found address {address} for {hostname}", address, hostname));

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();

                    Log.Debug("Using address {address} for {hostname}", selectedAddress, hostname);

                    return selectedAddress;
                }
            }
            catch
            {
                throw new UnresolvedHostnameException("Could not resolve the following hostname: {0}", hostname);
            }

            throw new UnresolvedHostnameException("Could not resolve the following hostname: {0}", hostname);
        }

    }
}
