using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

/*
 * 
 * 
 */

namespace ToyGE
{
    public class Block
    {
        //free list max item is 64KB, pre offset is 64
        //const int freeCount = 1024;

        IntPtr _headAddr;
        IntPtr _tailAddr;
        IntPtr[] _freeList;
        int _blockLength;
        List<Object> _indexs;
        Int16 _defaultGap;

        /// <summary>
        /// Initializes a new instance of the block.
        /// </summary>
        /// <param name="blockLength">Length of the block.</param>
        /// <param name="maxItemLength">Maximum length of the item.</param>
        /// <param name="indexs">Index of the p.</param>
        public Block(int blockLength, int maxItemLength, List<Object> indexs, Int16 defaultGap)
        {
            IntPtr memAddr = Marshal.AllocHGlobal(blockLength);

            _headAddr = memAddr;
            _tailAddr = memAddr;
            _freeList = new IntPtr[maxItemLength / 8];
            _blockLength = blockLength;
            _indexs = indexs;
            _defaultGap = defaultGap;
        }

        /// <summary>
        /// Get the memAddr by key
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <param name="key"></param>
        /// <param name="result"></param>
        /// <param name="index">the subscript of indexs</param>
        /// <returns></returns>
        private bool GetAddrWithKey<K>(K key, out IntPtr result, int index = 0)
        {
            if ((_indexs[index] as Index<K>).Search(key, out result) == false)
                return false;
            else
                return true;
        }

        /// <summary>
        /// get the free space and return newAddr
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="count"></param>
        /// <param name="cellSize">size of one cell</param>
        /// <param name="newAddr"></param>
        /// <returns></returns>
        private bool GetFreeSpace(IntPtr memAddr, int count, int cellSize, out IntPtr newAddr)
        {
            //get the length of object(string/list)
            int memLength = MemTool.GetTotalMemlengthByCount(count, _defaultGap, cellSize);

            //get newAddr of freeSpace
            newAddr = MemFreeList.GetFreeInBlock(_freeList, _headAddr, ref _tailAddr, _blockLength, memLength);
            if (newAddr.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(newAddr.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));
            return true;
        }

        #region BlockString

        public string BlockStringGet(ref IntPtr memAddr)
        {
            return MemString.Get(ref memAddr);
        }

        public string BlockStringGet<K>(K key, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey<K>(key, out memAddr, index) == false)
                return string.Empty;
            else
                return BlockStringGet(ref memAddr);
        }

        public bool BlockStringSet(ref IntPtr memAddr, string source)
        {
            IntPtr stringAddr;
            if (GetFreeSpace(memAddr, source.Length, sizeof(byte), out stringAddr) == false)
                return false;
            return MemString.Set(ref stringAddr, source, _defaultGap);
        }

        public bool BlockStringSet<K>(K key, ref IntPtr memAddr, string source, int index = 0)
        {
            if ((_indexs[index] as Index<K>).Insert(key, memAddr) == false)
                return false;
            return BlockStringSet(ref memAddr, source);
        }

        public bool BlockStringDelete(ref IntPtr memAddr)
        {
            //get string values begin addr
            IntPtr stringAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //backup offsetMemAddr as free begin addr
            IntPtr stringAddrBackup = stringAddr;

            //get status
            byte status = MemByte.Get(ref stringAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref stringAddr);

            //get nextOffsetAddr
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetHasNext(status) == true)
            {
                nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetToalMemLengthbyFullLength(fullLength);
            MemFreeList.InsertFreeList(stringAddrBackup, totalMemLength, _freeList);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return BlockStringDelete(ref nextOffsetAddr);
            }

            return true;
        }

        public bool BlockStringDelete<K>(K key, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            (_indexs[index] as Index<K>).Delete(key);
            return BlockStringDelete(ref memAddr);
        }

        public bool BlockStringUpdate(ref IntPtr memAddr, string value)
        {
            //get string values begin addr
            IntPtr stringAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            IntPtr statusAddr = stringAddr;
            byte status = MemByte.Get(ref stringAddr);
            bool hasNext = MemStatus.GetHasNext(status);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref stringAddr);

            //set status and get set length
            int setLength;
            if (value.Length > fullLength)
            {
                MemStatus.SetIsFull(statusAddr, true);
                MemStatus.SetHasNext(statusAddr, true);
                setLength = fullLength;
            }
            else if (value.Length == fullLength)
            {
                MemStatus.SetIsFull(statusAddr, true);
                MemStatus.SetHasNext(statusAddr, false);
                setLength = fullLength;
            }
            else
            {
                MemStatus.SetIsFull(statusAddr, false);
                MemStatus.SetHasNext(statusAddr, false);
                setLength = value.Length;
            }

            //set chars
            MemTool.SetChars(stringAddr, value, setLength);

            if (value.Length > fullLength)
            {
                //get rest string
                string nextValue = value.Substring(fullLength);

                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);

                //recursion update/set
                if (hasNext == true)
                    return BlockStringUpdate(ref nextOffsetAddr, nextValue);
                else
                {
                    BlockStringSet(ref stringAddr, nextValue);
                }
            }
            else
            {
                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);

                //remove next part
                if (hasNext == true)
                    return BlockStringDelete(ref nextOffsetAddr);
            }

            return true;
        }

        public bool BlockStringUpdate<K>(K key, string value, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return BlockStringUpdate(ref memAddr, value);
        }
        #endregion BlockString

        #region BlockList

        public List<T> BlockListGet<T>(ref IntPtr memAddr, Delegate<T>.GetItem_Structure getItem_Structure = null)
        {
            return MemList.Get<T>(ref memAddr, getItem_Structure);
        }

        public List<T> BlockListGet<K, T>(K key, Delegate<T>.GetItem_Structure getItem_Structure = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return null;
            else
                return BlockListGet<T>(ref memAddr, getItem_Structure);
        }

        public bool BlockListSet<T>(ref IntPtr memAddr, List<T> source, Delegate<T>.InsertItem_Structure insertItem_Structure = null)
        {
            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

            IntPtr listAddr;
            if (GetFreeSpace(memAddr, source.Count, sizeofT, out listAddr) == false)
                return false;

            return MemList.Set(ref listAddr, source, _defaultGap, insertItem_Structure);
        }

        public bool BlockListSet<K, T>(K key, ref IntPtr memAddr, List<T> source, Delegate<T>.InsertItem_Structure insertItem_Structure = null, int index = 0)
        {
            if ((_indexs[index] as Index<K>).Insert(key, memAddr) == false)
                return false;
            return BlockListSet<T>(ref memAddr, source, insertItem_Structure);
        }

        public bool BlockListDelete<T>(ref IntPtr memAddr, Delegate<T>.DeleteItem_Structure deleteItem_Structure = null)
        {
            //get list values begin addr
            IntPtr listAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //backup offsetMemAddr as free begin addr 
            IntPtr listAddrBackup = listAddr;

            //get status
            byte status = MemByte.Get(ref listAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref listAddr);

            //get lastAddr/nextOffsetAddr
            IntPtr lastAddr;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                lastAddr = listAddr + fullLength;
            else if (MemStatus.GetHasNext(status) == true)
            {
                lastAddr = listAddr + fullLength;
                nextOffsetAddr = MemTool.GetNextOffsetAddr(listAddr, fullLength);
            }
            else
            {
                //not full
                Int16 curLength = MemTool.GetCurLength(listAddr, fullLength);
                lastAddr = listAddr + curLength;
            }

            //delete items
            if (typeof(T) == typeof(String))
            {
                while (listAddr.ToInt64() < lastAddr.ToInt64())
                {
                    if (BlockStringDelete(ref listAddr) == false)
                        return false;
                }
            }
            else
            {
                while (listAddr.ToInt64() < lastAddr.ToInt64())
                {
                    if (deleteItem_Structure(ref listAddr) == false)
                        return false;
                }
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetToalMemLengthbyFullLength(fullLength);
            MemFreeList.InsertFreeList(listAddrBackup, totalMemLength, _freeList);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return BlockListDelete<T>(ref nextOffsetAddr, deleteItem_Structure);
            }

            return true;
        }

        public bool BlockListDelete<K, T>(K key, Delegate<T>.DeleteItem_Structure deleteItem_Structure = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return BlockListDelete<T>(ref memAddr, deleteItem_Structure);
        }

        public bool BlockListAdd<T>(IntPtr memAddr, T item, Delegate<T>.InsertItem_Structure insertItem_Structure = null)
        {
            //get offseted addr
            IntPtr listAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            IntPtr statusAddr = listAddr;
            byte status = MemByte.Get(ref listAddr);

            //get list byteLength
            Int16 fullLength = MemInt16.Get(ref listAddr);

            //add content
            if (MemStatus.GetIsFull(status) == true)
            {
                if (MemStatus.GetHasNext(status) == true)
                {
                    //get addr to recursion
                    IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(listAddr, fullLength);

                    return BlockListAdd<T>(nextOffsetAddr, item, insertItem_Structure);
                }
                else
                {
                    //update status hasNext
                    MemStatus.SetHasNext(statusAddr, true);

                    //get addr to insert new list
                    IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(listAddr, fullLength);

                    //generate new list<T> source
                    List<T> inputs = new List<T>() { item };

                    //insert new list into nextOffset
                    return BlockListSet<T>(ref nextOffsetAddr, inputs, insertItem_Structure);
                }
            }
            else
            {
                IntPtr curLengthAddr = MemTool.GetCurLengthAddr(listAddr, fullLength);
                Int16 curLength = MemTool.GetCurLength(listAddr, fullLength);
                IntPtr insertAddr = listAddr + curLength;

                //insert item
                if (typeof(T) == typeof(byte))
                    MemByte.Set(ref insertAddr, Convert.ToByte(item));
                else if (typeof(T) == typeof(Int16))
                    MemInt16.Set(ref insertAddr, Convert.ToInt16(item));
                else if (typeof(T) == typeof(Int32))
                    MemInt32.Set(ref insertAddr, Convert.ToInt32(item));
                else if (typeof(T) == typeof(Int64))
                    MemInt64.Set(ref insertAddr, Convert.ToInt64(item));
                else if (typeof(T) == typeof(String))
                {
                    if (BlockStringSet(ref insertAddr, item.ToString()) == false)
                        return false;
                }
                else
                {
                    if (insertItem_Structure(ref insertAddr, item) == false)
                        return false;
                }

                //update isFull
                if (listAddr + fullLength == insertAddr)
                {
                    MemStatus.SetIsFull(statusAddr, true);
                }
            }

            return true;
        }

        public bool BlockListAdd<K, T>(K key, T item, Delegate<T>.InsertItem_Structure insertItem_Structure = null, int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey<K>(key, out memAddr, index) == false)
                return false;
            else
                return BlockListAdd(memAddr, item, insertItem_Structure);
        }

        public unsafe bool BlockListRemove<T>(IntPtr memAddr, int index, Delegate<T>.DeleteItem_Structure deleteItem_Structure = null)
        {
            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

            //get offseted addr
            IntPtr listAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            IntPtr statusAddr = listAddr;
            byte status = MemByte.Get(ref listAddr);

            //get list byteLength
            Int16 fullLength = MemInt16.Get(ref listAddr);

            //get the distance of index item
            int indexDistance = sizeofT * index;

            //get the real length of body
            Int16 bodyLength = MemStatus.GetIsFull(status) ? fullLength : MemTool.GetCurLength(listAddr, fullLength);

            if (bodyLength > indexDistance)
            {
                //remove the item
                IntPtr removeAddr = listAddr + indexDistance;
                if (typeof(T).IsValueType == true)
                    removeAddr += sizeofT;
                else if (typeof(T) == typeof(String))
                {
                    if (BlockStringDelete(ref removeAddr) == false)
                        return false;
                }
                else
                {
                    if (deleteItem_Structure(ref removeAddr) == false)
                        return false;
                }

                //move other items forward
                MemTool.Memcpy(listAddr.ToPointer(), removeAddr.ToPointer(), bodyLength);

                //update curLength
                IntPtr curLengthAddr = MemTool.GetCurLengthAddr(listAddr, fullLength);
                MemInt16.Set(ref curLengthAddr, (Int16)(bodyLength - sizeofT));

                //set isFull=false
                if (MemStatus.GetIsFull(status) == true)
                    MemStatus.SetIsFull(statusAddr, false);
            }
            else
            {
                int nextIndex = index - bodyLength / sizeofT;

                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(listAddr, fullLength);

                return BlockListRemove<T>(nextOffsetAddr, nextIndex, deleteItem_Structure);
            }

            return true;
        }

        public bool BlockListRemove<K, T>(K key, int indexInList, Delegate<T>.DeleteItem_Structure deleteItem_Structure = null,int index = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrWithKey(key, out memAddr, index) == false)
                return false;
            else
                return BlockListRemove<T>(memAddr, index, deleteItem_Structure);
        }
        #endregion BlockList
    }
}
