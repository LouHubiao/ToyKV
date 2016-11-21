using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

/*
 * this file contains block operate functions with different structures
 * 
 */

namespace ToyGE
{
    public class Block
    {
        //free list max item is 64KB, pre offset is 64
        //const int freeCount = 1024;

        public IntPtr headAddr;
        public IntPtr tailAddr;
        public IntPtr[] freeList;
        public int blockLength;
        public List<Object> indexs;
        public Int16 defaultGap;
        public Dictionary<string, StructureHelper> structures;

        /// <summary>
        /// Initializes a new instance of the block.
        /// </summary>
        /// <param name="blockLength">Length of the block.</param>
        /// <param name="maxItemLength">Maximum length of the item.</param>
        /// <param name="indexs">Index of the p.</param>
        public Block(int blockLength, int maxItemLength, List<Object> indexs, Dictionary<string, StructureHelper> structures, Int16 defaultGap)
        {
            IntPtr memAddr = Marshal.AllocHGlobal(blockLength);

            headAddr = memAddr;
            tailAddr = memAddr;
            freeList = new IntPtr[maxItemLength / 8];
            this.blockLength = blockLength;
            this.indexs = indexs;
            this.structures = structures;
            this.defaultGap = defaultGap;
        }

        #region index operation

        public bool GetAddrWithKey<K>(K key, out IntPtr result, int index = 0)
        {
            if ((indexs[index] as Index<K>).Search(key, out result) == false)
                return false;
            else
                return true;
        }

        #endregion index operation

        #region freeList Operation
        /// <summary>
        /// insert memAddr part into freeList linedlist with length
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="length"></param>
        /// <param name="freeList"></param>
        public unsafe void InsertFree(IntPtr memAddr, int length)
        {
            //get index in freeList, 8byte is length of pointer
            int index = length / 8;

            //back up memAddr
            IntPtr memAddrBackup = memAddr;

            //add into freelist
            MemInt64.Set(ref memAddr, freeList[index].ToInt64());
            freeList[index] = memAddrBackup;
        }

        /// <summary>
        /// get free space with memLength in freelist or tailAddr
        /// </summary>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="memLength"></param>
        /// <returns></returns>
        unsafe IntPtr GetFree(int memLength)
        {
            //get index in freeList, 8byte is length of pointer
            int index = (memLength + 7) / 8;

            if (index > 0 && index < freeList.Length && freeList[index].ToInt64() != 0)
            {
                //get the 
                IntPtr memAddr = freeList[index];
                IntPtr memAddrBackup = memAddr;
                //update freeList
                freeList[index] = new IntPtr(MemInt64.Get(ref memAddrBackup));
                return memAddr;
            }
            else if (headAddr.ToInt64() + blockLength - tailAddr.ToInt64() > memLength)
            {
                //update tailLength
                IntPtr tailAddrCopy = tailAddr;
                tailAddr += memLength;
                return tailAddrCopy;
            }
            else
            {
                //no enough space in this block
                return new IntPtr(0);
            }
        }

        /// <summary>
        /// get free space and set pointer
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="memLength"></param>
        /// <param name="newAddr">new begin addr</param>
        /// <returns></returns>
        public bool GetNewSpace(ref IntPtr memAddr, int memLength, out IntPtr newAddr)
        {
            //get newAddr of freeSpace
            newAddr = GetFree(memLength);
            if (newAddr.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(newAddr.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));
            return true;
        }
        #endregion freeList Operation

        #region BlockString

        public string StringGet<K>(K key, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey<K>(key, out memAddr, index) == false)
                return string.Empty;
            else
                return MemString.Get(memAddr);
        }

        public bool StringSet<K>(K key, IntPtr memAddr, string source, int index = 0)
        {
            if ((indexs[index] as Index<K>).Insert(key, memAddr) == false)
                return false;
            return MemString.Set(memAddr, source, this);
        }

        public bool StringDelete<K>(K key, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            (indexs[index] as Index<K>).Delete(key);
            return MemString.Delete(memAddr, this);
        }

        public bool StringUpdate<K>(K key, string value, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return MemString.Update(memAddr, value, this);
        }
        #endregion BlockString

        #region BlockList

        public List<T> ListGet<K, T>(K key, StructureHelper structureHelper = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return null;
            else
                return MemList.Get<T>(memAddr, structureHelper);
        }

        public bool ListSet<K, T>(K key, ref IntPtr memAddr, List<T> source, StructureHelper structureHelper = null, int index = 0)
        {
            if ((indexs[index] as Index<K>).Insert(key, memAddr) == false)
                return false;
            return MemList.Set<T>(memAddr, source, this, structureHelper);
        }

        public bool ListDelete<K, T>(K key, StructureHelper structureHelper = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return MemList.Delete<T>(memAddr, this, structureHelper);
        }

        public bool ListAdd<K, T>(K key, T item, StructureHelper structureHelper = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey<K>(key, out memAddr, index) == false)
                return false;
            else
                return MemList.Add<T>(memAddr, item, this, structureHelper);
        }

        public bool ListRemove<K, T>(K key, int indexInList, StructureHelper structureHelper = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return MemList.Remove<T>(memAddr, indexInList, this, structureHelper);
        }
        #endregion BlockList

        #region Block Structure

        public Structure StructureGet<K>(K key, StructureHelper structureHelper, int[] headerIndexs = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey<K>(key, out memAddr, index) == false)
                return null;
            else
                return structureHelper.Get(memAddr, headerIndexs);
        }

        #endregion Block Structure
    }
}