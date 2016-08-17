using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    class UserMain
    {
        //hash index b-tree, contains cellID and logistic address
        public static Index<string> hashTree = new Index<string>(stringCompare, stringGetDefault);
        static int stringCompare(string val1, string val2)
        {
            return string.CompareOrdinal(val1, val2);
        }
        static string stringGetDefault()
        {
            return "";
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
        static Int16 gap = 10;

        //last addr in cur block
        static IntPtr curAddr = new IntPtr(0);

        //cur block index
        static int curBlockIndex = 0;

        //insert one node into memory and b-tree
        public static void InsertUser_Cell_Index(string cellID, List<Int64> txs)
        {
            if (blockAddrs.Count == 0)
            {
                CreateBlock();
                insertIndex(cellID);
                insertCell(txs);
            }
            else
            {
                int txsLength = txs.Count * sizeof(Int64);
                if (curAddr.ToInt64() - blockAddrs[curBlockIndex].ToInt64() > (perBlockSize - txsLength))
                {
                    if (freeAddrs[curBlockIndex][txsLength].ToInt64() != 0)
                    {
                        IntPtr curAddrCopy = curAddr;
                        curAddr = freeAddrs[curBlockIndex][txsLength];
                        MemHelper.DeleteFromFreelist(curAddr, freeAddrs[curBlockIndex]);

                        insertIndex(cellID);
                        insertCell(txs);

                        //recover curAddr
                        curAddr = curAddrCopy;
                    }
                    else
                    {
                        CreateBlock();
                        insertIndex(cellID);
                        insertCell(txs);
                    }
                }
                else
                {
                    insertIndex(cellID);
                    insertCell(txs);
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
        static void insertIndex(string cellID)
        {
            hashTree.BTInsert(ref hashTree.root, cellID, curAddr);
        }

        //insert cell content into memory
        static void insertCell(List<Int64> txs)
        {
            UserHelper.InsertCell(txs, ref curAddr, ref preAddr, gap);
            blockCounts[curBlockIndex]++;
        }

        //search node by key
        public static List<Int64> SearchNode(string key)
        {
            IntPtr node = new IntPtr();
            if (hashTree.BTSearch(hashTree.root, key, ref node))
            {
                if (!MemHelper.IsDeleted(node))
                {
                    List<Int64> result = UserHelper.GetCell(node);
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
        public static void DeleteNode(string key)
        {
            IntPtr nodeAddr = new IntPtr();
            if (hashTree.BTSearch(hashTree.root, key, ref nodeAddr))
            {
                //get whichi block the nodeAddr in
                int blockIndex = GetBlockIndex(nodeAddr);
                //reduce count in block, for foreach
                blockCounts[blockIndex] -= 1;

                UserHelper.DeleteCell(nodeAddr, freeAddrs[blockIndex]);
                MemHelper.ConsoleFree(freeAddrs[blockIndex]);
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
