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

        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        /// <exception cref="SocketException">Thrown if none of the Default Brokers can be contacted.</exception>
        /// <exception cref="KafkaApplicationException">Thrown if none of the Default Brokers can be contacted.</exception>
        public async Task<T> SendProtocolRequest<T>(IKafkaRequest<T> request, string topic, int partition) where T : class,IBaseResponse
        {
            //Log Start  Sesion  Id
            DateTime topicMetadataRefreshDate = DateTime.MinValue;
            T response = null;
            int retryTime = 0;

            while (retryTime < _maxRetry)
            {
                bool needToRefreshTopicMetadata;
                ExceptionDispatchInfo socketException = null;
                try
                {
                    await _brokerRouter.RefreshMissingTopicMetadata(topic);
                    topicMetadataRefreshDate = _brokerRouter.GetTopicMetadataRefreshTime(topic);
                    //find route it can chage after Metadata Refresh
                    var route = _brokerRouter.SelectBrokerRouteFromLocalCache(topic, partition);
                    var responses = await route.Connection.SendAsync(request);
                    response = responses.FirstOrDefault();
                    if (response == null)
                    {
                        return null; /*this can happened if you send ProduceRequest with ack level=0 */
                    }

                    var error = (ErrorResponseCode)response.Error;
                    if (error == ErrorResponseCode.NoError) { return response; }

                    //It means we had an error 
                    needToRefreshTopicMetadata = CanRecoverByRefreshMetadata(error);

                }
                catch (SocketException ex)
                {
                    socketException = ExceptionDispatchInfo.Capture(ex);
                    needToRefreshTopicMetadata = true;
                }
                bool hasMoreRetry = retryTime + 1 < _maxRetry;
                if (needToRefreshTopicMetadata && hasMoreRetry)
                {
                    //Log Retry Sesion id
                    retryTime++;
                    await TryRefreshTopicMetadata(topic, topicMetadataRefreshDate, socketException, response);
                }
                else
                {
                    throwError(socketException, response);
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

        private async Task TryRefreshTopicMetadata(string topic, DateTime lastTopicMetadataRefreshDate,
            ExceptionDispatchInfo socketException, IBaseResponse response)
        {
            bool metadataNotExpire = !await _brokerRouter.RefreshTopicMetadata(topic);//Do Not Change this order
            bool metadataNotUpdate = _brokerRouter.GetTopicMetadataRefreshTime(topic) == lastTopicMetadataRefreshDate;
    


            if (metadataNotUpdate && metadataNotExpire)
            {
                throwError(socketException, response);
            }
        }

        private static void throwError(ExceptionDispatchInfo socketException, IBaseResponse response)
        {
            if (socketException != null)
            {
                socketException.Throw();
            }
            throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error)
            {
                ErrorCode = response.Error
            };
        }

        public void Dispose()
        {
            _brokerRouter.Dispose();
        }
    }
}