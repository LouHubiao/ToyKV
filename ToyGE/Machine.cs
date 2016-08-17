using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    //index node type
    public class MachineIndex<K>
    {
        public int machineID;   //IP address
        public Block<K> block;  //block info
    }

    public class Machines<K>
    {
        public int BlockSize = 0;

        //machine indexs, <hashBegin, MachineIndex>
        public Dictionary<Int16, MachineIndex<K>> machineIndex = new Dictionary<Int16, MachineIndex<K>>();

        //init machines, 
        public Machines(int blockSize, Dictionary<int, Int64> machineInfo)
        {
            BlockSize = blockSize;
            Int16 indexBegin = 0;
            int offset = (Int16.MaxValue - indexBegin) / machineInfo.Count();
            Int16 indexEnd = (Int16)(indexBegin + offset);

            foreach (var item in machineInfo)
            {
                //init one block
                int blockCount = (Int32)(item.Value / BlockSize);
                AddMachine(item.Key, blockCount, BlockSize, indexBegin, indexEnd);
                indexBegin = indexEnd;
                indexEnd = (Int16)(indexBegin + offset);
            }
        }

        void AddMachine(int machineID, int blockCount, int blockSize, Int16 indexBegin, Int16 indexEnd)
        {
            int offset = (indexEnd - indexBegin) / blockCount;
            Int16 beginKey = indexBegin;
            //init blocks
            for (int i = 0; i < blockCount; i++)
            {
                //add block into machineIndex
                MachineIndex<K> index = new MachineIndex<K>();
                index.machineID = machineID;

                index.block = new Block<K>(blockSize);        //alloc block
                machineIndex.Add(beginKey, index);
                beginKey = (Int16)(beginKey + offset);
            }
        }

        //get addr and block info by key
        public static bool Get(Dictionary<Int16, MachineIndex<K>> machineIndex, K key, Delegate<K>.CompareT compare, out IntPtr cellAddr, out Block<K> block)
        {
            //search in machineIndex and get machineID and blockInfo
            Int16 hash = (Int16)key.GetHashCode();
            hash = (hash > 0) ? hash : (Int16)(-hash);
            Int16 closest = 0;
            foreach (var item in machineIndex)
            {
                if (item.Key < hash && item.Key >= closest)
                    closest = item.Key;
            }
            MachineIndex<K> index = machineIndex[closest];

            //get block info
            block = index.block;

            //if(index.machineID==0||index.machineID==selfIP)
            //else send to target machine

            //search in index and get addr
            if (B_Tree<K, IntPtr>.Search(block.blockIndex.root, key, compare, out cellAddr) == false)
                return false;

            return true;
        }
    }
}
