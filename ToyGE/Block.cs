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
    public class Block<K>
    {
        //free list max item is 64KB, pre offset is 64
        //const int freeCount = 1024;

        IntPtr _headAddr;
        IntPtr _tailAddr;
        IntPtr[] _freeList;
        int _blockLength;
        List<Index<K>> _index;
        Int16 _defaultGap;

        /// <summary>
        /// Initializes a new instance of the block.
        /// </summary>
        /// <param name="blockLength">Length of the block.</param>
        /// <param name="maxItemLength">Maximum length of the item.</param>
        /// <param name="index">Index of the p.</param>
        public Block(int blockLength, int maxItemLength, List<Index<K>> index, Int16 defaultGap)
        {
            IntPtr memAddr = Marshal.AllocHGlobal(blockLength);

            _headAddr = memAddr;
            _tailAddr = memAddr;
            _freeList = new IntPtr[maxItemLength / 8];
            _blockLength = blockLength;
            _index = index;
            _defaultGap = defaultGap;
        }

        private bool GetAddrByKey(K key, out IntPtr result, int keyIndex = 0)
        {
            if (_index[keyIndex].Search(key, out result) == false)
                return false;
            else
                return true;
        }

        public string GetStringByKey(K key, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return string.Empty;
            else
                return MemString.Get(ref memAddr);
        }

        public bool SetStringByKey(K key, string source, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemString.Set(ref memAddr, source, _freeList, _headAddr, ref _tailAddr, _blockLength, _defaultGap);
        }

        public bool UpdateStringByKey(K key, string value, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemString.Update(ref memAddr, value, _freeList, _headAddr, ref _tailAddr, _blockLength, _defaultGap);
        }

        public List<T> GetListByKey<T>(K key, Delegate<T>.GetItem getItem, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return null;
            else
                return MemList.Get<T>(ref memAddr, getItem);
        }

        public bool SetListByKey<T>(K key, List<T> source, Delegate<T>.InsertItem_Object insertItem_Object, Delegate<T>.InsertItem_Value insertItem_Value, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemList.Set<T>(ref memAddr, source, _freeList, _headAddr, ref _tailAddr, _blockLength, _defaultGap, insertItem_Object, insertItem_Value);
        }

        public bool DeleteListByKey<T>(K key, Delegate<T>.DeleteItem_Object deleteItem_Object, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemList.Delete<T>(ref memAddr, _freeList, deleteItem_Object);
        }

        public bool AddListByKey<T>(K key, T item, Delegate<T>.InsertItem_Object insertItem_Object,
            Delegate<T>.InsertItem_Value insertItem_Value, int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemList.Add<T>(memAddr, item, _freeList, _headAddr, ref _tailAddr, _blockLength, _defaultGap, insertItem_Object, insertItem_Value);
        }

        public bool RemoveAtIndexListByKey<T>(K key, int index, Delegate<T>.DeleteItem_Object deleteItem_Object,
            int keyIndex = 0)
        {
            IntPtr memAddr = IntPtr.Zero;
            if (GetAddrByKey(key, out memAddr, keyIndex) == false)
                return false;
            else
                return MemList.RemoveAtIndex<T>(memAddr, index, _freeList, _headAddr, ref _tailAddr, _blockLength, _defaultGap, deleteItem_Object);
        }
    }
}
