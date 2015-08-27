using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace KafkaNet
{
    public class ProtocolGateway : IDisposable
    {
        private readonly IBrokerRouter _brokerRouter;

        //Add Loger
        public ProtocolGateway(params Uri[] brokerUrl)
        {
            var kafkaOptions = new KafkaOptions(brokerUrl) { MaximumReconnectionTimeout = TimeSpan.FromSeconds(60), ResponseTimeoutMs = TimeSpan.FromSeconds(60) };
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        public ProtocolGateway(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public ProtocolGateway(KafkaOptions kafkaOptions)
        {
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        private readonly int _maxRetry = 2;

        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing</exception>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the default brokers can be contacted</exception>
        /// <exception cref="ResponseTimeoutException">Thrown if there request times out</exception>
        /// <exception cref="SocketException">Thrown in case of network error contacting broker (after retries)</exception>
        /// <exception cref="KafkaApplicationException">Thrown in case of an unexpected error in the request</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown in case of Topic is not valid</exception>
        public async Task<T> SendProtocolRequest<T>(IKafkaRequest<T> request, string topic, int partition) where T : class,IBaseResponse
        {
            ValidateTopic(topic);
            T response = null;
            int retryTime = 0;

            while (retryTime < _maxRetry)
            {
                bool needToRefreshTopicMetadata;
                ExceptionDispatchInfo exception = null;
                try
                {
                    await _brokerRouter.RefreshMissingTopicMetadata(topic);

                    //find route it can chage after Metadata Refresh
                    var route = _brokerRouter.SelectBrokerRouteFromLocalCache(topic, partition);
                    var responses = await route.Connection.SendAsync(request);
                    response = responses.FirstOrDefault();

                    //this can happened if you send ProduceRequest with ack level=0
                    if (response == null)
                    {
                        return null;
                    }

                    var error = (ErrorResponseCode)response.Error;
                    if (error == ErrorResponseCode.NoError)
                    {
                        return response;
                    }

                    //It means we had an error

                    needToRefreshTopicMetadata = CanRecoverByRefreshMetadata(error);
                }
                catch (ResponseTimeoutException ex)
                {
                    exception = ExceptionDispatchInfo.Capture(ex);
                    needToRefreshTopicMetadata = true;
                }
                catch (SocketException ex)
                {
                    exception = ExceptionDispatchInfo.Capture(ex);
                    needToRefreshTopicMetadata = true;
                }
                bool hasMoreRetry = retryTime + 1 < _maxRetry;

                if (needToRefreshTopicMetadata && hasMoreRetry)
                {
                    retryTime++;
                    await _brokerRouter.RefreshTopicMetadata(topic);
                }
                else
                {
                    if (exception != null)
                    {
                        exception.Throw();
                    }

                    throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
                }
            }

            throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error);
        }

        private static bool CanRecoverByRefreshMetadata(ErrorResponseCode error)
        {
            return error == ErrorResponseCode.BrokerNotAvailable ||
                                         error == ErrorResponseCode.ConsumerCoordinatorNotAvailableCode ||
                                         error == ErrorResponseCode.LeaderNotAvailable ||
                                         error == ErrorResponseCode.NotLeaderForPartition;
        }

        public void Dispose()
        {
            _brokerRouter.Dispose();
        }

        private void ValidateTopic(string topic)
        {
            if (topic.Contains(" "))
            {
                throw new ArgumentOutOfRangeException("Topic is not valid");
            }
        }
    }
}