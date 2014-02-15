using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka_net.Common
{
    public class FailCrcCheckException : Exception
    {
        public FailCrcCheckException(string message) : base(message) { }
    }
}
