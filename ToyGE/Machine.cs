using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace ToyGE
{
    

    //index node type
    public class MachineIndex<K>
    {
        public UInt32 machineIP;   //IP address
        public Block<K> block;  //block info
    }

    public class Machines<K>
    {
        public int BlockSize = 0;

        //machine indexs, <hashBegin, MachineIndex>
        public Dictionary<Int16, MachineIndex<K>> machineIndexs = new Dictionary<Int16, MachineIndex<K>>();

        public string ConfigurationManager { get; private set; }

        //init machines, machineInventory is ip and memory space
        public Machines(int blockSize, Dictionary<UInt32, int> machineInventory)
        {
            this.BlockSize = blockSize;
            Int16 indexBegin = 0;
            int offset = (Int16.MaxValue - indexBegin) / machineInventory.Count();
            Int16 indexEnd = (Int16)(indexBegin + offset);
            //local ip to 0
            var host = Dns.GetHostEntry(Dns.GetHostName());
            UInt32 localIP = 0;
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    localIP = BitConverter.ToUInt32(ip.GetAddressBytes(), 0);
                    break;
                }
            }
            Console.WriteLine(localIP);
            foreach (var item in machineInventory)
            {
                //init one block
                int blockCount = (Int32)(item.Value / BlockSize);
                if (item.Key == localIP)
                    AddMachine(0, blockCount, BlockSize, indexBegin, indexEnd);
                else
                    AddMachine(item.Key, blockCount, BlockSize, indexBegin, indexEnd);
                indexBegin = indexEnd;
                indexEnd = (Int16)(indexBegin + offset);
            }
        }

        void AddMachine(UInt32 machineIP, int blockCount, int blockSize, Int16 indexBegin, Int16 indexEnd)
        {
            int offset = (indexEnd - indexBegin) / blockCount;
            Int16 beginKey = indexBegin;
            //init blocks
            for (int i = 0; i < blockCount; i++)
            {
                //add block into machineIndex
                MachineIndex<K> index = new MachineIndex<K>();
                index.machineIP = machineIP;

                index.block = new Block<K>(blockSize);        //alloc block
                machineIndexs.Add(beginKey, index);
                beginKey = (Int16)(beginKey + offset);
            }
        }

        //get addr and block info by key
        public static bool Get(Dictionary<Int16, MachineIndex<K>> machineIndexs, K key, Delegate<K>.CompareT compare, out IntPtr cellAddr, out MachineIndex<K> machineIndex)
        {
            //search in machineIndex and get machineID and blockInfo
            Int16 hash = (Int16)key.GetHashCode();
            hash = (hash > 0) ? hash : (Int16)(-hash);
            Int16 closest = 0;
            foreach (var item in machineIndexs)
            {
                if (item.Key < hash && item.Key >= closest)
                    closest = item.Key;
            }
            MachineIndex<K> index = machineIndexs[closest];

            //get index
            machineIndex = index;

            //in remote machine
            if (index.machineIP != 0)
            {
                cellAddr = IntPtr.Zero;
                return false;
            }

            //search in index and get addr
            return BTree<>.SearchInSubTree(index.block.blockIndex.root, key, compare, out cellAddr);
        }
    }
}
