using KafkaNet.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class ProtocolGateway
    {
        private BrokerRouter _brokerRouter;

        public ProtocolGateway(params Uri[] brokerUrl)
        {
            _brokerRouter = new BrokerRouter(new KafkaOptions(brokerUrl));
        }

        private readonly int _maxRetry = 2;
        public async Task<T> SendProtocolRequest<T>(IKafkaRequest<T> request, string topic, int partition) where T : class
        {
            int retryTime = 0;
            while (retryTime <= _maxRetry)
            {
                try
                {
                    var route = _brokerRouter.SelectBrokerRoute(topic, partition);
                    var responses = await route.Connection.SendAsync(request);
                    var response = responses.FirstOrDefault();
                    IBaseResponse baseResponse = response as IBaseResponse;
                    if (baseResponse != null && (ErrorResponseCode)baseResponse.Error == ErrorResponseCode.NotLeaderForPartition)
                    {
                        _brokerRouter.RefreshTopicMetadata();
                    }
                    else
                    {
                        return response;
                    }
                }
                catch (Exception)
                {
                    throw;
                    //TODO:need to refresh if  has socket error indicating the client cannot communicate with a particular broker,
                }
                retryTime++;
            }

            return null;


        }
    }
}