namespace KafkaNet
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using KafkaNet.Model;
    using KafkaNet.Protocol;

    public class PartitionConsumer : CommonQueries
    {
        private readonly ConsumerOptions _options;

        private readonly Action<Message> _messageFetchedCallback;

        private bool _disposed;

        private Tuple<Task, CancellationTokenSource> _consumeTask;

        public PartitionConsumer(ConsumerOptions options, string topic, int partitionId, Action<Message> messageFetchedCallback):base(options.Router)
        {
            this._options = options;
            this._messageFetchedCallback = messageFetchedCallback;
            this.PartitionId = partitionId;
            this.Topic = topic;
            this.FetchSize = Fetch.DefaultMaxBytes;
        }

        public string Topic { get; private set; }

        public int PartitionId { get; private set; }

        public long Offset { get; set; }

        public int FetchSize { get; private set; }

        public void Stop()
        {
            if (_consumeTask != null)
            {
                this._consumeTask.Item2.Cancel();
                this._consumeTask.Item1.Wait();
                this._consumeTask = null;
            }
        }

        public void Start()
        {
            if (_consumeTask == null)
            {
                var cancelTokenSource = new CancellationTokenSource();
                this._consumeTask = new Tuple<Task, CancellationTokenSource>(Task.Factory.StartNew(this.Consume, cancelTokenSource.Token), cancelTokenSource);
            }
        }

        private void Consume()
        {
            while (!this._consumeTask.Item2.Token.IsCancellationRequested)
            {
                try
                {
                    //build fetch for each item in the batchSize
                    var fetchRequest = CreateFetchRequest();

                    //make request and post to queue
                    var responses = this.FetchResponses(fetchRequest);

                    if (responses.Count > 0)
                    {
                        var response = responses[0]; //we only asked for one response
                        var messages = response.Messages;

                        if (response.Error != 0)
                        {
                            if (response.Error == (short)ErrorResponseCode.OffsetOutOfRange)
                            {
                                this.FixOffsetRangeError(fetchRequest, response);
                            }
                        }
                        else
                        {
                            try
                            {
                                foreach (var message in messages)
                                {
                                    _messageFetchedCallback(message);

                                    Offset = message.Meta.Offset + 1;
                                }
                            }
                            catch (InsufficientDataException ex)
                            {
                                if (ex.ExpectedSize > FetchSize) FetchSize = ex.ExpectedSize * 2;
                            }
                        }

                        continue;
                    }

                    Thread.Sleep(100);
                }
                catch (Exception ex)
                {
                    _options.Log.ErrorFormat("Exception occured while polling topic:{0} partition:{1}.  Polling will continue.  Exception={2}", Topic, PartitionId, ex);
                }
            }
         
        }

        private FetchRequest CreateFetchRequest()
        {
            var fetches = new List<Fetch>
                              {
                                  new Fetch
                                      {
                                          Topic = this.Topic,
                                          PartitionId = PartitionId,
                                          Offset = this.Offset,
                                          MaxBytes = FetchSize
                                      }
                              };

            var fetchRequest = new FetchRequest { Fetches = fetches };

            return fetchRequest;
        }

        private List<FetchResponse> FetchResponses(FetchRequest fetchRequest)
        {
            var route = this._options.Router.SelectBrokerRoute(Topic, PartitionId);

            return route.Connection.SendAsync(fetchRequest).Result;
        }

        private void FixOffsetRangeError(FetchRequest fetchRequest, FetchResponse response)
        {
            var fetch = fetchRequest.Fetches[0];

            Func<OffsetTime, long> getOffset =
                offsetTime =>
                this.GetTopicOffsetAsync(response.Topic, 1, (int)offsetTime).Result.First(r => r.PartitionId == fetch.PartitionId).Offsets[0];

            var latest = getOffset(OffsetTime.Latest);
            var earliest = getOffset(OffsetTime.Earliest);

            var asked = fetch.Offset;

            if (asked < earliest) Offset = earliest;
            else if (asked > latest) Offset = latest;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                this.Stop();
            }

            base.Dispose(disposing);
            _disposed = true;
        }
    }
}