using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*	
    In Memory:

    User {
        status      byte
        nextNode    int32   // next node
        preNode     int32   // pre node
        txs         int32   // =>txs
    }

    txs{
        status      byte
        length      int16
        cellID      Int64[]
        [curLnegth] int32
        [nextPart]  int32
    }
*/

namespace ToyGE
{
    class UserHelper
    {
        //convert memory user's txs into List<Int64>
        public static List<Int64> GetCell(IntPtr userAddr)
        {
            // jump cellStatus/ cellNextNode/ cellPreNode
            MemHelper.addrJump(ref userAddr, 9);

            //read txs
            List<Int64> userTxs = MemHelper.GetList<Int64>(ref userAddr, MemHelper.GetInt64);

            return userTxs;
        }

        //convert jsonback to byte[] in memory
        public static void InsertCell(List<Int64> userTxs, ref IntPtr memAddr, ref IntPtr preAddr, Int16 gap)
        {
            //update nextNode and preNode
            MemHelper.UpdateNextNode_PreNode(memAddr, preAddr);
            preAddr = memAddr;

            //pointer for insert unsure length type, 45 is the length of tx
            IntPtr nextPartAddr = memAddr + 13;

            //insert cellStatus
            MemHelper.InsertValue(ref memAddr, (byte)0);

            //jump nextNode and preNode, has updated
            MemHelper.addrJump(ref memAddr, 8);

            //insert txs(X)
            MemHelper.InsertEntireList(ref memAddr, userTxs, ref nextPartAddr, sizeof(Int32), gap, MemHelper.InsertValue, null);

            memAddr = nextPartAddr;
        }

        //delete user cell
        public static unsafe void DeleteCell(IntPtr memAddr, IntPtr[] freeAddrs)
        {
            //update status IsDelete=1
            IntPtr statusAddr = memAddr;
            byte* status = (byte*)(statusAddr.ToPointer());
            byte mask = 0x80;
            *status = (byte)(*status | mask);

            //delete txs
            IntPtr outsAddr = memAddr + 9;
            MemHelper.DeleteList<In>(ref outsAddr, freeAddrs, null);

            //update cell link list
            int length = 13;
            if (length >= 64)
            {
                IntPtr nextAddr = new IntPtr(memAddr.ToInt64() + *(Int32*)(memAddr + 1));
                Int32 preOffset = *(Int32*)(memAddr + 5);
                IntPtr preAddr = preOffset == 0 ? new IntPtr(0) : new IntPtr(memAddr.ToInt64() - preOffset);
                MemHelper.UpdateNextNode_PreNode(nextAddr, preAddr);
            }
        }


    }
}
