using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    class Statistic
    {
        public static unsafe bool Amount_Statistic(IntPtr curAddr)
        {
            byte status = MemByte.Get(ref curAddr);
            byte mask = 0x80;
            if ((status & mask) != 0)
                return false;

            Int64* amount = (Int64*)((curAddr + 28).ToPointer());
            if (*amount >= 1000000000)
            {
                return true;
            }
            return false;
        }

        public static unsafe bool Count_Statistic(IntPtr curAddr)
        {
            //judge isDeleted
            byte status = MemByte.Get(ref curAddr);
            byte mask = 0x80;
            if ((status & mask) != 0)
                return false;
            return true;
        }
    }
}
