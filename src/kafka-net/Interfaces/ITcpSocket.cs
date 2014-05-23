using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet.Interfaces
{
    public interface ITcpSocket
    {
        void Connect(Uri server, int port);
        
        Task<byte[]> ReadAsync(int readSize);
    }
}
