using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

/*
 * this file contains memory operate functions with different structures
 * 
 * now support: Byte, Int16, Int32, Int64, String, List, Struct
 * 
 * memory object struct:
 *  string: status(8)| fullLength(16)| Body| [CurLength(16)]| [NextOffset(32)]|
 *  list:   status(8)| fullLength(16)| Body| [CurLength(16)]| [NextOffset(32)]|
 *  struct: status(8)| Body|
 *  
 * status:
 *  string: hasNext| isFull| ...
 *  list:   hasNext| isFull| ...
 *  struct: ...
 *  
 * ps:
 *  1.fullLength is the length of Body, curLength is the length of content
 *  2.use pointer if not value type
 */

namespace ToyGE
{
    /// <summary>
    /// the usual tool about memory management of different structure
    /// </summary>
    public class MemTool
    {
        /// <summary>
        /// Get next address by offset, result = memAddr + sizeof(offset) + *(memAddr)
        /// </summary>
        /// <param name="memAddr">Addr Before Offset</param>
        /// <returns>offseted address</returns>
        public static unsafe IntPtr GetAddrByAddrBeforeOffset(ref IntPtr memAddr)
        {
            Int32 offset = MemInt32.Get(ref memAddr);
            //memAddrBeforeOffset has been changed
            return memAddr + offset;
        }

        /// <summary>
        /// get the curLengthAddr, result = memAddr + fullLength
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="fullLength"></param>
        /// <returns></returns>
        public static unsafe IntPtr GetCurLengthAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength;
        }

        /// <summary>
        /// get the curLength value, result = *(memAddr + fullLength)
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength"></param>
        /// <returns></returns>
        public static unsafe Int16 GetCurLength(IntPtr memAddr, Int16 fullLength)
        {
            IntPtr curLengthAddr = memAddr + fullLength;
            return MemInt16.Get(ref curLengthAddr);
        }

        /// <summary>
        /// set the curLength value, *(memAddr + fullLength) = curLength
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength"></param>
        /// <param name="curLength"></param>
        public static unsafe void SetCurLength(IntPtr memAddr, Int16 fullLength, Int16 curLength)
        {
            IntPtr curLengthAddr = memAddr + fullLength;
            MemInt16.Set(ref curLengthAddr, curLength);
        }

        /// <summary>
        /// get nextOffset addr, result = memAddr + fullLength + sizeof(curLength)
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength"></param>
        /// <returns></returns>
        public static unsafe IntPtr GetNextOffsetAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength + sizeof(Int16);
        }

        /// <summary>
        /// get the nextOffset value, result = *(memAddr + fullLength + sizeof(curLength))
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength"></param>
        /// <returns></returns>
        public static unsafe Int32 GetNextOffset(IntPtr memAddr, Int16 fullLength)
        {
            IntPtr nextOffsetAddr = memAddr + fullLength + sizeof(Int16);
            return MemInt32.Get(ref nextOffsetAddr);
        }

        /// <summary>
        /// set the nextOffset value, *(memAddr + fullLength + sizeof(curLength)) = curLength
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength">fullLength</param>
        /// <param name="nextOffset"></param>
        public static unsafe void SetNextOffset(IntPtr memAddr, Int16 fullLength, Int32 nextOffset)
        {
            IntPtr nextOffsetAddr = memAddr + fullLength + sizeof(Int16);
            MemInt32.Set(ref nextOffsetAddr, nextOffset);
        }

        /// <summary>
        /// jump interval
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="interval"></param>
        public static unsafe void addrJump(ref IntPtr memAddr, int interval)
        {
            memAddr += interval;
        }

        /// <summary>
        /// get the fullLength for string and list, reuslt = (count + gap) * sizeof(T)
        /// </summary>
        /// <param name="count"></param>
        /// <param name="gap"></param>
        /// <returns></returns>
        public static unsafe Int16 GetFullLength(int count, Int16 gap, int sizeofT)
        {
            return (Int16)((count + gap) * sizeofT);
        }

        /// <summary>
        /// get the total memory length for string or list, result = sizeof(status)+sizeof(fullLength) + fullLength + sizeof(curLength) + sizeof(nextOffset)
        /// </summary>
        /// <param name="count">The count.</param>
        /// <param name="gap">The gap.</param>
        /// <param name="sizeofT">The sizeof t.</param>
        /// <returns>System.Int32.</returns>
        public static unsafe int GetTotalMemlengthByCount(int count, Int16 gap, int sizeofT)
        {
            int result = sizeof(Byte) + sizeof(Int16);
            Int16 fullLength = GetFullLength(count, gap, sizeofT);
            result += fullLength + sizeof(Int16) + sizeof(Int32);
            return fullLength;
        }

        /// <summary>
        /// get the total memory length for string and list, result = sizeof(byte) + sizeof(Int16) + fullLength + sizeof(Int16) + sizeof(Int32)
        /// </summary>
        /// <param name="fullLength">fullLength in string or list</param>
        /// <returns></returns>
        public static unsafe int GetToalMemLengthbyFullLength(Int16 fullLength)
        {
            return sizeof(byte) + sizeof(Int16) + fullLength + sizeof(Int16) + sizeof(Int32);
        }

        /// <summary>
        /// get the value size of T
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static int GetValueTypeSize(Type t)
        {
            if (t == typeof(byte))
                return 1;
            else if (t == typeof(Int16))
                return 2;
            else if (t == typeof(Int32))
                return 4;
            else if (t == typeof(Int64))
                return 8;
            return 0;
        }

        /// <summary>
        /// memcpy implement by C#
        /// </summary>
        /// <param name="dest">dest addr</param>
        /// <param name="src">src addr</param>
        /// <param name="size">copy size</param>
        public static unsafe void Memcpy(void* dest, void* src, int size)
        {
            if (size > 0)
            {
                byte* destPtr = (byte*)dest;
                byte* srcPtr = (byte*)src;
                for (int i = 0; i < size; i++)
                {
                    *destPtr = *srcPtr;
                    destPtr++;
                    srcPtr++;
                }
            }
        }
    }

    //byte operation
    public class MemByte
    {
        public static unsafe byte Get(ref IntPtr memAddr)
        {
            byte* result = (byte*)(memAddr.ToPointer());
            memAddr += sizeof(byte);
            return *result;
        }

        public static unsafe void Set(ref IntPtr memAddr, byte value)
        {
            *(byte*)(memAddr.ToPointer()) = value;
            memAddr += sizeof(byte);
        }

        public static unsafe void Jump(ref IntPtr memAddr)
        {
            memAddr += sizeof(byte);
        }
    }

    //int16 operation
    public class MemInt16
    {
        public static unsafe Int16 Get(ref IntPtr memAddr)
        {
            Int16* result = (Int16*)(memAddr.ToPointer());
            memAddr += sizeof(Int16);
            return *result;
        }

        public static unsafe void Set(ref IntPtr memAddr, Int16 value)
        {
            *(Int16*)(memAddr.ToPointer()) = value;
            memAddr += sizeof(Int16);
        }

        public static unsafe void Jump(ref IntPtr memAddr)
        {
            memAddr += sizeof(Int16);
        }
    }

    //int32 operation
    public class MemInt32
    {
        public static unsafe Int32 Get(ref IntPtr memAddr)
        {
            Int32* result = (Int32*)(memAddr.ToPointer());
            memAddr += sizeof(Int32);
            return *result;
        }

        public static unsafe void Set(ref IntPtr memAddr, Int32 value)
        {
            *(Int32*)(memAddr.ToPointer()) = value;
            memAddr += sizeof(Int32);
        }

        public static unsafe void Jump(ref IntPtr memAddr)
        {
            memAddr += sizeof(Int32);
        }
    }

    //int64 operation
    public class MemInt64
    {
        public static unsafe Int64 Get(ref IntPtr memAddr)
        {
            Int64* result = (Int64*)(memAddr.ToPointer());
            memAddr += sizeof(Int64);
            return *result;
        }

        public static unsafe void Set(ref IntPtr memAddr, Int64 value)
        {
            *(Int64*)(memAddr.ToPointer()) = value;
            memAddr += sizeof(Int64);
        }

        public static unsafe void Jump(ref IntPtr memAddr)
        {
            memAddr += sizeof(Int64);
        }
    }

    //status operation
    public class MemStatus
    {
        /// <summary>
        /// return true if memory part has next content
        /// </summary>
        /// <param name="status">status of structure</param>
        /// <returns></returns>
        public static bool GetHasNext(byte status)
        {
            byte mask = 0x80;
            byte hasNext = (byte)(status & mask);
            return hasNext > 0;
        }

        /// <summary>
        /// return true if memory part has full content
        /// </summary>
        /// <param name="status">status of structure</param>
        /// <returns></returns>
        public static bool GetIsFull(byte status)
        {
            byte mask = 0x40;
            byte isFull = (byte)(status & mask);
            return isFull > 0;
        }

        public unsafe static void SetHasNext(IntPtr memAddr, bool HasNext)
        {
            IntPtr memAddrCopy = memAddr;
            byte status = MemByte.Get(ref memAddrCopy);
            if (HasNext == true)
            {
                byte mask = 0x80;
                status = (byte)(status | mask);
            }
            else
            {
                byte mask = 0x7F;
                status = (byte)(status & mask);
            }
            MemByte.Set(ref memAddr, status);
        }

        public unsafe static void SetIsFull(IntPtr memAddr, bool isFull)
        {
            IntPtr memAddrCopy = memAddr;
            byte status = MemByte.Get(ref memAddrCopy);
            if (isFull == true)
            {
                byte mask = 0x40;
                status = (byte)(status | mask);
            }
            else
            {
                byte mask = 0xBF;
                status = (byte)(status & mask);
            }
            MemByte.Set(ref memAddr, status);
        }
    }

    //string operation
    public class MemString
    {
        /// <summary>
        /// get entire string
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <returns>System.String.</returns>
        public static string Get(ref IntPtr memAddr)
        {
            StringBuilder result = new StringBuilder();

            //get string values begin addr
            IntPtr stringAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            byte status = MemByte.Get(ref stringAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref stringAddr);

            //get lastAddr/nextOffsetAddr
            Int16 contentLength;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                contentLength = fullLength;
            else if (MemStatus.GetHasNext(status) == true)
            {
                contentLength = fullLength;
                nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);
            }
            else
            {
                //not full
                contentLength = MemTool.GetCurLength(stringAddr, fullLength);
            }

            //add string into result
            result.Append(GetChars(stringAddr, contentLength));

            //recursion next part string
            if (MemStatus.GetHasNext(status) == true)
            {
                result.Append(MemString.Get(ref nextOffsetAddr));
            }

            return result.ToString();
        }

        /// <summary>
        /// insert Entire string, must has enough space
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="source">The source.</param>
        /// <param name="freeList">The free list.</param>
        /// <param name="headAddr">The head addr.</param>
        /// <param name="tailAddr">The tail addr.</param>
        /// <param name="blockLength">Length of the block.</param>
        /// <param name="defaultGap">The default gap.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public static bool Set(ref IntPtr memAddr, string source, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap)
        {
            //get the length of T
            int sizeofT = sizeof(byte);

            //get ListAddr in this block, only for list, maybe not enough for all items
            int memLength = MemTool.GetTotalMemlengthByCount(source.Length, defaultGap, sizeofT);
            IntPtr stringAddr = MemFreeList.GetFreeInBlock(freeList, headAddr, ref tailAddr, blockLength, memLength);
            if (stringAddr.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(stringAddr.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //insert status
            if (defaultGap != 0)
                MemByte.Set(ref stringAddr, (byte)0x00);
            else
                MemByte.Set(ref stringAddr, (byte)0x20);

            //insert length with gap
            Int16 fullLength = MemTool.GetFullLength(source.Length, defaultGap, sizeofT);
            MemInt16.Set(ref stringAddr, fullLength);

            //insert string content
            SetChars(stringAddr, source, source.Length);

            //set curLength
            if (defaultGap != 0)
            {
                MemTool.SetCurLength(stringAddr, fullLength, (Int16) source.Length);
            }

            return true;
        }

        /// <summary>
        /// delete string content
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="freeList">The free list.</param>
        public static unsafe bool Delete(ref IntPtr memAddr, IntPtr[] freeList)
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
            MemFreeList.InsertFreeList(stringAddrBackup, totalMemLength, freeList);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return MemString.Delete(ref nextOffsetAddr, freeList);
            }

            return true;
        }

        /// <summary>
        /// update string
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="value">The value.</param>
        /// <param name="freeList">The free list.</param>
        /// <param name="headAddr">The head addr.</param>
        /// <param name="tailAddr">The tail addr.</param>
        /// <param name="blockLength">Length of the block.</param>
        /// <param name="defaultGap">The default gap.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public static unsafe bool Update(ref IntPtr memAddr, string value, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap)
        {
            //get string values begin addr
            IntPtr stringAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            IntPtr statusAddr = stringAddr;
            byte status = MemByte.Get(ref stringAddr);
            bool hasNext = MemStatus.GetHasNext(status);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref stringAddr);

            //

            //set status and get set length
            int setLength;
            if (value.Length > fullLength)
            {
                MemStatus.SetIsFull(statusAddr, true);
                MemStatus.SetHasNext(statusAddr, true);
                setLength = fullLength;
            }
            else if(value.Length == fullLength)
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
            SetChars(stringAddr, value, setLength);

            if (value.Length > fullLength)
            {
                //get rest string
                string nextValue = value.Substring(fullLength);

                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);

                //recursion update/set
                if (hasNext == true)
                    return MemString.Update(ref nextOffsetAddr, nextValue, freeList, headAddr, ref tailAddr, blockLength,
                            defaultGap);
                else
                    return MemString.Set(ref stringAddr, nextValue, freeList, headAddr, ref tailAddr, blockLength,
                            defaultGap);
            }
            else
            {
                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(stringAddr, fullLength);

                //remove next part
                if (hasNext == true)
                    return MemString.Delete(ref nextOffsetAddr, freeList);
            }

            return true;
        }

        /// <summary>
        /// get string content with length
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="length">The length.</param>
        /// <returns>System.String.</returns>
        private static unsafe string GetChars(IntPtr memAddr, Int16 length)
        {
            if (length > 0)
            {
                byte[] resultBytes = new byte[length];
                fixed (void* dest = &resultBytes[0])
                {
                    MemTool.Memcpy(dest, memAddr.ToPointer(), length);
                }
                return System.Text.Encoding.ASCII.GetString(resultBytes);
            }
            return string.Empty;
        }

        /// <summary>
        /// isnert string content
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="source">The source.</param>
        private static unsafe void SetChars(IntPtr memAddr, string source, int length)
        {
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(source);
            fixed (void* src = &bytes[0])
            {
                MemTool.Memcpy(memAddr.ToPointer(), src, length);
            }
        }
    }

    //list operation
    public class MemList
    {
        /// <summary>
        /// get entire list, getItem like GetString()
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="getItem"></param>
        /// <returns></returns>
        public static List<T> Get<T>(ref IntPtr memAddr, Delegate<T>.GetItem getItem)
        {
            List<T> result = new List<T>();

            //get list values begin addr
            IntPtr ListAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            byte status = MemByte.Get(ref ListAddr);

            //get length
            Int16 fullLength = MemInt16.Get(ref ListAddr);

            //get lastAddr/nextOffsetAddr
            IntPtr lastAddr;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                lastAddr = ListAddr + fullLength;
            else if (MemStatus.GetHasNext(status) == true)
            {
                lastAddr = ListAddr + fullLength;
                nextOffsetAddr = MemTool.GetNextOffsetAddr(ListAddr, fullLength);
            }
            else
            {
                //not full
                Int16 curLength = MemTool.GetCurLength(ListAddr, fullLength);
                lastAddr = ListAddr + curLength;
            }

            //add item into list
            while (ListAddr.ToInt64() < lastAddr.ToInt64())
                result.Add(getItem(ref ListAddr));

            //recursion next part list
            if (MemStatus.GetHasNext(status) == true)
            {
                result.AddRange(MemList.Get<T>(ref nextOffsetAddr, getItem));
            }

            return result;
        }

        /// <summary>
        /// insert list, return false if failed
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="source"></param>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="defaultGap"></param>
        /// <param name="insertItem_Object"></param>
        /// <param name="insertItem_Value"></param>
        /// <returns></returns>
        public static bool Set<T>(ref IntPtr memAddr, List<T> source, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap, Delegate<T>.InsertItem_Object insertItem_Object, Delegate<T>.InsertItem_Value insertItem_Value)
        {
            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

            //get ListAddr in this block, only for list, maybe not enough for all items
            int memLength = MemTool.GetTotalMemlengthByCount(source.Count, defaultGap, sizeofT);
            IntPtr listAddr = MemFreeList.GetFreeInBlock(freeList, headAddr, ref tailAddr, blockLength, memLength);
            if (listAddr.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(listAddr.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //insert status
            if (defaultGap != 0)
                MemByte.Set(ref listAddr, (byte)0x00);
            else
                MemByte.Set(ref listAddr, (byte)0x20);

            //insert length with gap
            Int16 fullLength = MemTool.GetFullLength(source.Count, defaultGap, sizeofT);
            MemInt16.Set(ref listAddr, fullLength);

            //backup nextFreeInBlock
            IntPtr listAddrBackup = listAddr;

            //insert list part context
            if (typeof(T).IsValueType)
            {
                for (int i = 0; i < source.Count; i++)
                {
                    insertItem_Value(ref listAddr, source[i]);
                }
            }
            else
            {
                for (int i = 0; i < source.Count; i++)
                {
                    if (insertItem_Object(ref listAddr, source[i], freeList, headAddr, ref tailAddr, blockLength, defaultGap) == false)
                        return false;
                }
            }

            //insert curLength
            if (defaultGap != 0)
            {
                Int16 curLength = (Int16)(listAddr.ToInt64() - listAddrBackup.ToInt64());
                MemTool.SetCurLength(listAddrBackup, fullLength, curLength);
            }

            return true;
        }

        /// <summary>
        /// delete list content, return false if failed
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="freeList"></param>
        /// <param name="deleteItem_Object"></param>
        public static bool Delete<T>(ref IntPtr memAddr, IntPtr[] freeList, Delegate<T>.DeleteItem_Object deleteItem_Object)
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
            if (typeof(T).IsValueType == false)
            {
                while (listAddr.ToInt64() < lastAddr.ToInt64())
                {
                    if (deleteItem_Object(ref listAddr, freeList) == false)
                        return false;
                }
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetToalMemLengthbyFullLength(fullLength);
            MemFreeList.InsertFreeList(listAddrBackup, totalMemLength, freeList);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return MemList.Delete<T>(ref nextOffsetAddr, freeList, deleteItem_Object);
            }

            return true;
        }

        /// <summary>
        /// add an item into list, return false if failed
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="item"></param>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="defaultGap"></param>
        /// <param name="insertItem_Object"></param>
        /// <param name="insertItem_Value"></param>
        /// <returns></returns>
        public static bool Add<T>(IntPtr memAddr, T item, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap, Delegate<T>.InsertItem_Object insertItem_Object, Delegate<T>.InsertItem_Value insertItem_Value)
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

                    return MemList.Add<T>(nextOffsetAddr, item, freeList, headAddr, ref tailAddr, blockLength, defaultGap, insertItem_Object, insertItem_Value);
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
                    return MemList.Set<T>(ref nextOffsetAddr, inputs, freeList, headAddr, ref tailAddr, blockLength, defaultGap, insertItem_Object, insertItem_Value);
                }
            }
            else
            {
                IntPtr curLengthAddr = MemTool.GetCurLengthAddr(listAddr, fullLength);
                Int16 curLength = MemTool.GetCurLength(listAddr, fullLength);
                IntPtr insertAddr = listAddr + curLength;

                //insert item
                if (typeof(T).IsValueType)
                {
                    insertItem_Value(ref insertAddr, item);
                    MemInt16.Set(ref curLengthAddr, (Int16)(insertAddr.ToInt64() - listAddr.ToInt64()));
                }
                else
                {
                    if (insertItem_Object(ref insertAddr, item, freeList, headAddr, ref tailAddr, blockLength, defaultGap) == false)
                        return false;
                    MemInt16.Set(ref curLengthAddr, (Int16)(curLength + sizeof(Int32)));
                }

                if (listAddr + fullLength == insertAddr)
                {
                    MemStatus.SetIsFull(statusAddr, true);
                }
            }

            return true;
        }

        /// <summary>
        /// remove an item in list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="index"></param>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="defaultGap"></param>
        /// <param name="deleteItem_Object"></param>
        /// <returns></returns>
        public static unsafe bool RemoveAtIndex<T>(IntPtr memAddr, int index, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap, Delegate<T>.DeleteItem_Object deleteItem_Object)
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
                else
                {
                    if (deleteItem_Object(ref removeAddr, freeList) == false)
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

                return RemoveAtIndex<T>(nextOffsetAddr, nextIndex, freeList, headAddr, ref tailAddr, blockLength, defaultGap, deleteItem_Object);
            }

            return true;
        }
    }

    //free memory operation
    public class MemFreeList
    {
        //temp, log free list
        public static void ConsoleFree(IntPtr[] freeAdds)
        {
            for (int i = 0; i < freeAdds.Length; i++)
            {
                IntPtr first = freeAdds[i];
                if (first.ToInt64() != 0)
                {
                    IntPtr curAddr = first;
                    while (curAddr.ToInt64() != 0)
                    {
                        Console.WriteLine("Line:" + i + ",addr:" + curAddr);
                        IntPtr nextAddr = new IntPtr(MemInt64.Get(ref curAddr));
                        curAddr = nextAddr;
                    }
                }
            }
        }

        /// <summary>
        /// insert memAddr part into freeList linedlist with length
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="length"></param>
        /// <param name="freeList"></param>
        public static unsafe void InsertFreeList(IntPtr memAddr, int length, IntPtr[] freeList)
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
        /// get free space from current free list or memory block, return 0 if has not enough space
        /// </summary>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="memLength"></param>
        /// <returns></returns>
        public static unsafe IntPtr GetFreeInBlock(IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, int memLength)
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
    }

    //user defined structure operation
    public interface MemStructure
    {
        /// <summary>
        /// Gets structure
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="headerIndex">Index of the header.</param>
        /// <returns>MemStructure.</returns>
        MemStructure Get(ref IntPtr memAddr, int[] headerIndex);

        /// <summary>
        /// insert structure
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="source">The source.</param>
        /// <param name="headerIndex">Index of the header.</param>
        /// <param name="freeList">The free list.</param>
        /// <param name="headAddr">The head addr.</param>
        /// <param name="tailAddr">The tail addr.</param>
        /// <param name="blockLength">Length of the block.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        bool Set(ref IntPtr memAddr, MemStructure source, int[] headerIndex, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr,
            Int32 blockLength);

        /// <summary>
        /// Deletes structure
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="freeList">The free list.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        bool Delete(ref IntPtr memAddr, IntPtr[] freeList);
    }
}
