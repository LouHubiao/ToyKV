using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    class TxMain
    {
        //hash index b-tree, contains cellID and logistic address
        static Index<Int64> hashTree = new Index<Int64>(int64Compare, int64GetDefault);
        static int int64Compare(Int64 val1, Int64 val2)
        {
            return (int)(val1 - val2);
        }
        static Int64 int64GetDefault()
        {
            return -1;
        }

        //memory parts begin address
        static List<IntPtr> blockAddrs = new List<IntPtr>();

        //memory parts node count
        static List<int> blockCounts = new List<int>();

        //free memory list for every block
        static List<IntPtr[]> freeAddrs = new List<IntPtr[]>();

        //current last tail addr for every block
        static List<IntPtr> tailAddrs = new List<IntPtr>();

        //previous cell Addr
        static IntPtr preAddr = new IntPtr(0);

        //1GB per memory block size
        static Int32 perBlockSize = 1 << 30;

        //gap for every string or list
        static Int16 gap = 0;

        //last addr in cur block
        static IntPtr curAddr = new IntPtr(0);

        //cur block index
        static int curBlockIndex = 0;

        //insert one node into memory and b-tree
        public static void InsertTx_Cell_Index(Int64 cellID ,TX jsonBack)
        {
            if (blockAddrs.Count == 0 )
            {
                CreateBlock();
                insertIndex(cellID);
                insertCell(jsonBack);
            }
            else 
            {
                int jsonBackLength = jsonBack.GetLength();
                if (curAddr.ToInt64() - blockAddrs[curBlockIndex].ToInt64() > (perBlockSize - jsonBackLength))
                {
                    if (freeAddrs[curBlockIndex][jsonBackLength].ToInt64() != 0)
                    {
                        IntPtr curAddrCopy = curAddr;
                        curAddr = freeAddrs[curBlockIndex][jsonBackLength];
                        MemHelper.DeleteFromFreelist(curAddr, freeAddrs[curBlockIndex]);

                        insertIndex(cellID);
                        insertCell(jsonBack);

                        //recover curAddr
                        curAddr = curAddrCopy;
                    }
                    else
                    {
                        CreateBlock();
                        insertIndex(cellID);
                        insertCell(jsonBack);
                    }
                }
                else
                {
                    insertIndex(cellID);
                    insertCell(jsonBack);
                }
            }
        }

        static void CreateBlock()
        {
            IntPtr memAddr = Marshal.AllocHGlobal(perBlockSize);
            blockAddrs.Add(memAddr);
            tailAddrs.Add(memAddr);
            blockCounts.Add(0);
            freeAddrs.Add(new IntPtr[1 << 16]);
            preAddr = new IntPtr(0);

            curAddr = memAddr;
            curBlockIndex = blockAddrs.Count - 1;
        }

        //isnert cellID in b-tree
        static void insertIndex(Int64 cellID)
        {
            hashTree.BTInsert(ref hashTree.root, cellID, curAddr);
        }

        static void insertCell(TX jsonBack)
        {
            TxHelper.InsertCell(jsonBack, ref curAddr, ref preAddr, gap);
            blockCounts[curBlockIndex]++;
        }

        //search node by key
        public static TX SearchNode(Int64 key)
        {
            IntPtr node = new IntPtr();
            if (hashTree.BTSearch(hashTree.root, key, ref node))
            {
                if (!MemHelper.IsDeleted(node))
                {
                    TX result = TxHelper.GetCell(node);
                    return result;
                }
            }
            return null;
        }

        // Statistic some property
        public delegate bool StatisticFun(IntPtr memAddr, Int64 a, Int64 b);
        public static unsafe int Foreach(StatisticFun fun, Int64 amount, Int64 other)
        {
            int result = 0;
            for (int i = 0; i < blockAddrs.Count; i++)
            {
                IntPtr memAddr = blockAddrs[i];
                for (int j = 0; j < blockCounts[i]; j++)
                {
                    Int32* nextOffset = (Int32*)(memAddr + 1);
                    if (fun(memAddr, amount, other))
                    {
                        if (!MemHelper.IsDeleted(memAddr))
                            result++;
                        else
                            j--;
                    }
                    if (*nextOffset == 0)
                        break;
                    memAddr += *nextOffset;
                }
            }
            return result;
        }

        //delete node
        public static void DeleteNode(Int64 key)
        {
            IntPtr nodeAddr = new IntPtr();
            if (hashTree.BTSearch(hashTree.root, key, ref nodeAddr))
            {
                //get whichi block the nodeAddr in
                int blockIndex = GetBlockIndex(nodeAddr);
                //reduce count in block, for foreach
                blockCounts[blockIndex] -= 1;

                TxHelper.DeleteCell(nodeAddr, freeAddrs[blockIndex]);
                MemHelper.ConsoleFree(freeAddrs[blockIndex]);
            }
        }

        //update amount in tx
        public static void UpdateAmount(Int64 key, Int64 newAmount)
        {
            IntPtr nodeAddr = new IntPtr();
            if (hashTree.BTSearch(hashTree.root, key, ref nodeAddr))
            {
                TxHelper.UpdateAmount(nodeAddr, newAmount);
            }
        }

        public static void UpdateHash(Int64 key, string newHash)
        {
            IntPtr nodeAddr = new IntPtr();
            //search index
            if (hashTree.BTSearch(hashTree.root, key, ref nodeAddr))
            {
                int blockIndex = GetBlockIndex(nodeAddr);
                IntPtr[] freeAddr = freeAddrs[blockIndex];
                TxHelper.UpdateHash(nodeAddr, newHash, freeAddr);
            }
        }

        //get the block index of cellAddr
        static int GetBlockIndex(IntPtr cellAddr)
        {
            for (int i = 0; i < blockAddrs.Count; i++)
            {
                if (cellAddr.ToInt64() >= blockAddrs[i].ToInt64())
                {
                    return i;
                }
            }
            return -1;
        }
    }
}
