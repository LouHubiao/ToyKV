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
    public class BlockInt64
    {
        //freelist max item is 64KB, pre offset is 64
        const int freeCount = 1024;

        public IntPtr headAddr;
        public IntPtr tailAddr;
        public IntPtr[] freeList;
        public int blockLength;
        public ARTInt64 blockIndex;

        public BlockInt64(int blockLength)
        {
            IntPtr memAddr = Marshal.AllocHGlobal(blockLength);

            this.headAddr = memAddr;
            this.tailAddr = memAddr;
            this.freeList = new IntPtr[freeCount];
            this.blockLength = blockLength;
            this.blockIndex = new ARTInt64();     //alloc blockIndex
        }
    }

    //index node type
    public class MachineIndexInt64
    {
        public UInt32 machineIP;   //IP address
        public BlockInt64 block;  //block info
    }

    public class MachinesInt64
    {
        public int BlockSize = 0;

        //machine indexs, <hashBegin, MachineIndex>
        public Dictionary<Int16, MachineIndexInt64> machineIndexs = new Dictionary<Int16, MachineIndexInt64>();

        public string ConfigurationManager { get; private set; }

        //init machines, machineInventory is ip and memory space
        public MachinesInt64(int blockSize, Dictionary<UInt32, Int64> machineInventory)
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
                MachineIndexInt64 index = new MachineIndexInt64();
                index.machineIP = machineIP;

                index.block = new BlockInt64(blockSize);        //alloc block
                machineIndexs.Add(beginKey, index);
                beginKey = (Int16)(beginKey + offset);
            }
        }

        //get addr and block info by key
        public static bool GetMachineIndex(Dictionary<Int16, MachineIndexInt64> machineIndexs, Int64 key, out MachineIndexInt64 machineIndex)
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
            MachineIndexInt64 index = machineIndexs[closest];

            //get index
            machineIndex = index;

            //in remote machine
            if (index.machineIP != 0)
                return false;

            //in local machine
            return true;
        }

        /// <summary>
        /// find cellAddr in machine index
        /// </summary>
        /// <param name="machineIndex"></param>
        /// <param name="key"></param>
        /// <param name="cellAddr"></param>
        /// <returns>has found</returns>
        public static bool GetCellAddr(MachineIndexInt64 machineIndex, Int64 key, out IntPtr cellAddr)
        {
            //search in index and get addr
            return ARTInt64.Search(machineIndex.block.blockIndex.tree, key, out cellAddr);
        }
    }
}
