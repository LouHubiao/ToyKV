using System;
using System.Collections.Generic;

/*	
    In Memory:

    Tx {
        status      byte
        CellID      Int64
        hash        int32   // =>hash
        time        Int64
        ins         int32   // =>ins
        outs        int32   // =>outs
        amount      Int64
    }

    hash{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }

    ins{
        status      byte
        length      int16
        context     int32[] //=>in
        [curLnegth] int32
        [nextPart]  int32
    }

    in{
        status      byte
        addr        int32   // =>in_addr
        tx_index    Int64
    }

    in_addr{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }

    outs{
        status      byte
        length      int16
        context     int32[] //=>out
        [curLnegth] int32
        [nextPart]  int32

    }

    out{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }
*/

namespace ToyGE
{
    public class TxHelper
    {
        #region search operation

        //convert memory Tx to object for random access
        public static bool Get(Int64 key, Dictionary<Int16, MachineIndex<Int64>> machineIndex, out TX tx)
        {
            //get tx Addr
            IntPtr cellAddr;
            Block<Int64> block;
            if (Machines<Int64>.Get(machineIndex, key, Compare.CompareInt64, out cellAddr, out block) == false)
            {
                tx = null;
                return false;
            }

            //get cell
            tx = new TX();

            // judge isDelete
            byte status = MemByte.Get(ref cellAddr);
            byte mask = 0x80;
            if ((status & mask) != 0)
            {
                tx = null;
                return false;
            }

            //read cellID
            tx.CellID = MemInt64.Get(ref cellAddr);

            //read hash
            tx.hash = MemString.Get(ref cellAddr);

            //read time
            tx.time = MemInt64.Get(ref cellAddr);

            //read ins
            tx.ins = MemList.Get<In>(ref cellAddr, GetIn);

            //read outs
            tx.outs = MemList.Get<string>(ref cellAddr, MemString.Get);

            //time amount
            tx.amount = MemInt64.Get(ref cellAddr);

            return true;
        }

        //get In struct
        public static In GetIn(ref IntPtr inAddr)
        {
            IntPtr offsetMemAddr = MemTool.GetOffsetedAddr(ref inAddr);

            byte status = MemByte.Get(ref offsetMemAddr);

            string addr = MemString.Get(ref offsetMemAddr);

            Int64 tx_index = MemInt64.Get(ref offsetMemAddr);

            return new In(addr, tx_index);
        }

        #endregion search operation


        #region insert operation
        //convert object to byte[] in memory
        public static bool Set(TX tx, Dictionary<Int16, MachineIndex<Int64>> machineIndex, Int16 gap)
        {
            //get block info
            IntPtr cellAddr;
            Block<Int64> block;
            Machines<Int64>.Get(machineIndex, tx.CellID, Compare.CompareInt64, out cellAddr, out block);

            //if has cell, update it
            if (cellAddr != IntPtr.Zero)
            {
                //update
            }

            //judge if has enough space for just cell 37
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, 37);
            if (nextFreeInBlock.ToInt64() == 0)
                return false;   //update false

            B_Tree<Int64, IntPtr>.Insert(ref block.blockIndex.root, tx.CellID, nextFreeInBlock, Compare.CompareInt64);

            //pointer for insert unsure length type, 37 is the length of tx
            IntPtr nextPartAddr = nextFreeInBlock + 37;

            //insert cellStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert CellID
            MemInt64.Set(ref nextFreeInBlock, tx.CellID);

            //insert hash(X)
            MemString.Set(ref nextFreeInBlock, tx.hash, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap);

            //insert time
            MemInt64.Set(ref nextFreeInBlock, tx.time);

            //insert ins(X)
            MemList.Set<In>(ref nextFreeInBlock, tx.ins, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, SetIn, null);

            //insert outs(X)
            MemList.Set<string>(ref nextFreeInBlock, tx.outs, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, MemString.Set, null);

            //insert amount
            MemInt64.Set(ref nextFreeInBlock, tx.amount);

            return true;
        }

        //insert In struct
        static bool SetIn(ref IntPtr memAddr, In input, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap)
        {
            //judge if has enough space for just cell 13
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(freeList, headAddr, ref tailAddr, blockLength, 13);
            if (nextFreeInBlock.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(nextFreeInBlock.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //struct length
            IntPtr nextNextPartAddr = nextFreeInBlock + 13;

            //insert inStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert in_addr
            MemString.Set(ref nextFreeInBlock, input.addr, freeList, headAddr, ref tailAddr, blockLength, gap);

            //insert tx_index
            MemInt64.Set(ref nextFreeInBlock, input.tx_index);

            return true;
        }
        #endregion insert operation

        #region delete operation
        //delete tx cell
        public static unsafe bool Delete(Int64 key, Dictionary<Int16, MachineIndex<Int64>> machineIndex)
        {
            //get tx Addr
            IntPtr cellAddr;
            Block<Int64> block;
            if (Machines<Int64>.Get(machineIndex, key, Compare.CompareInt64, out cellAddr, out block) == false)
            {
                return false;
            }

            //update status IsDelete=1
            IntPtr memAddrCopy = cellAddr;
            byte* status = (byte*)(memAddrCopy.ToPointer());
            byte mask = 0x80;
            *status = (byte)(*status | mask);

            //jump status and CellID
            memAddrCopy = memAddrCopy + 1 + 8;

            //delete hash
            MemString.Delete(ref memAddrCopy, block.freeList);

            //jump time
            memAddrCopy = memAddrCopy + 8;

            //delete ins
            MemList.Delete<In>(ref memAddrCopy, block.freeList, DeleteIn);

            ////update cell link list
            //int length = 44;
            //if (length >= 64)
            //{
            //    IntPtr nextAddr = new IntPtr(memAddr.ToInt64() + *(Int32*)(memAddr + 1));
            //    Int32 preOffset = *(Int32*)(memAddr + 5);
            //    IntPtr preAddr = preOffset == 0 ? new IntPtr(0) : new IntPtr(memAddr.ToInt64() - preOffset);
            //    MemCell.UpdateNextNode_PreNode(nextAddr, preAddr);
            //}

            return true;
        }

        public static void DeleteIn(ref IntPtr memAddr, IntPtr[] freeAddrs)
        {
            IntPtr offsetMemAddr = MemTool.GetOffsetedAddr(ref memAddr);

            //jump status
            offsetMemAddr = offsetMemAddr + 1;

            //delete in_addr
            MemString.Delete(ref offsetMemAddr, freeAddrs);
        }
        #endregion delete operation

        #region foreach
        //foreach the index fo statistic
        public static int Foreach(Dictionary<Int16, MachineIndex<Int64>> machineIndex, Delegate<Int64>.Statistic statistic)
        {
            int result = 0;
            foreach (MachineIndex<Int64> index in machineIndex.Values)
            {
                Block<Int64> block = index.block;
                Node<Int64, IntPtr> node = block.blockIndex.root;
                result += BTreeForeach(node, statistic);
            }
            return result;
        }

        //walking btree
        static int BTreeForeach(Node<Int64, IntPtr> node, Delegate<Int64>.Statistic statistic)
        {
            if (node == null || node.keys.Count == 0)
            {
                return 0;
            }

            int result = 0;
            for (int i = 0; i < node.keys.Count; i++)
            {
                if (statistic(node.values[i]) == true)
                {
                    result++;
                }

                if (node.kids.Count > i)
                    result += BTreeForeach(node.kids[i], statistic);
            }
            if (node.kids.Count > node.keys.Count)
                result += BTreeForeach(node.kids[node.keys.Count], statistic);
            return result;
        }
        #endregion foreach

        #region update operation
        //update hash
        //public static unsafe void UpdateHash(IntPtr memAddr, string newHash, IntPtr[] freeAdds)
        //{
        //    //pointer for hash
        //    memAddr += 17;
        //    MemString.Update(memAddr, newHash, freeAdds);
        //}

        ////update amount
        //public static unsafe void UpdateAmount(IntPtr memAddr, Int64 newAmount)
        //{
        //    //pointer for amount
        //    memAddr += 37;
        //    MemHelper.InsertValue(ref memAddr, newAmount);
        //}
        #endregion update operation
    }
}
