using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    //some delegate for index
    public class Delegate<K>
    {
        public delegate int CompareT(K t1, K t2);

        public delegate K GetItem(ref IntPtr memAddr);
        public delegate bool InsertItem_Object(ref IntPtr memAddr, K input, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap);
        public delegate bool InsertItem_Value(ref IntPtr memAddr, K input);
        public delegate bool Statistic(IntPtr memAddr);
    }

    public class Compare
    {
        public static int CompareInt64(Int64 val1, Int64 val2)
        {
            return (int)(val1 - val2);
        }
    }
}
