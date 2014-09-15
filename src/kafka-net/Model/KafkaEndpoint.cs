using System;
using System.Net;

namespace KafkaNet.Model
{
    public class KafkaEndpoint
    {
        public Uri ServeUri { get; set; }
        public IPEndPoint Endpoint { get; set; }

        protected bool Equals(KafkaEndpoint other)
        {
            return Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            //calculated like this to ensure ports on same address sort in the desc order
            return (Endpoint != null ? Endpoint.Address.GetHashCode() + Endpoint.Port : 0);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((KafkaEndpoint) obj);
        }

        public override string ToString()
        {
            return ServeUri.ToString();
        }
    }
}
