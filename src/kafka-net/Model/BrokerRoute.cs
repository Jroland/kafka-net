namespace KafkaNet
{
    public class BrokerRoute
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public IKafkaConnection Connection { get; set; }
        public override string ToString()
        {
            return string.Format("{0} Topic:{1} PartitionId:{2}", Connection.Endpoint.ServeUri, Topic, PartitionId);
        }

        #region Equals Override...
        protected bool Equals(BrokerRoute other)
        {
            return string.Equals(Topic, other.Topic) && PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Topic != null ? Topic.GetHashCode() : 0) * 397) ^ PartitionId;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((BrokerRoute)obj);
        }
        #endregion
    }
}