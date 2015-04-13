using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public enum OffsetSelectionStrategy
    {
        Earliest = -2,
        Last = -3,
        Next = -1,
        Specified = 0,
        NextUncommitted = -4,
    };

    public class TopicSelector
    {
        public string Topic { get; set; }
        public int Partition { get; set; }

        public long Offset { get; set; }
        
        public OffsetSelectionStrategy DefaultOffsetSelection { get; set; }
        public OffsetSelectionStrategy FailureOffsetSelection { get; set; }

        public TopicSelector()
        {
            DefaultOffsetSelection = OffsetSelectionStrategy.Specified;
            FailureOffsetSelection = OffsetSelectionStrategy.Next; 
        }

    }
}
