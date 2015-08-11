using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaNet
{
    public class ProtocolGateway : IDisposable
    {
        private readonly BrokerRouter _brokerRouter;

        public ProtocolGateway(params Uri[] brokerUrl)
        {
            var kafkaOptions = new KafkaOptions(brokerUrl) { MaximumReconnectionTimeout = TimeSpan.FromSeconds(60), ResponseTimeoutMs = TimeSpan.FromSeconds(60) };
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        public ProtocolGateway(KafkaOptions kafkaOptions)
        {
            _brokerRouter = new BrokerRouter(kafkaOptions);
        }

        private readonly int _maxRetry = 2;

        public async Task<T> SendProtocolRequest<T>(IKafkaRequest<T> request, string topic, int partition) where T : class,IBaseResponse
        {

            int retryTime = 0;
            while (retryTime <= _maxRetry)
            {
                try
                {
                    await _brokerRouter.RefreshMissingTopicMetadata(topic);
                    //find route it can chage after Metadata Refresh
                    var route = _brokerRouter.SelectBrokerRouteFromLocalCache(topic, partition);
                    var responses = await route.Connection.SendAsync(request);
                    var response = responses.FirstOrDefault();
                    if (response == null)
                    {
                        return null; /*this can happened if you send ProduceRequest with ack level=0 */
                    }

                    var error = (ErrorResponseCode)response.Error;
                    if (error == ErrorResponseCode.NoError)
                    {
                        return response;
                    }

                    bool needToRefreshTopicMetadata = error == ErrorResponseCode.BrokerNotAvailable ||
                                                      error == ErrorResponseCode.ConsumerCoordinatorNotAvailableCode ||
                                                      error == ErrorResponseCode.LeaderNotAvailable ||
                                                      error == ErrorResponseCode.NotLeaderForPartition;

                    if (needToRefreshTopicMetadata)
                    {
                        _brokerRouter.RefreshTopicMetadata();
                        /* what happened if multiple consumer Refresh Topic Metadata*/
                    }
                    else
                    {
                        throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}",
                            response.Error) { ErrorCode = response.Error };
                    }
                }
                catch (KafkaApplicationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    //log
                    _brokerRouter.RefreshTopicMetadata(); /* what happened if multiple consumer Refresh Topic Metadata*/
                }
                retryTime++;
            }
            throw new Exception("can not send message");
        }

        public void Dispose()
        {
            _brokerRouter.Dispose();
        }
    }
}