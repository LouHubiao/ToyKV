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
        public static unsafe IntPtr GetAddrByOffsetAddr(IntPtr memAddr)
        {
            Int32 offset = MemInt32.Get(ref memAddr);
            //memAddrBeforeOffset has been changed
            return memAddr + offset;
        }

        // get the curLengthAddr, result = memAddr + fullLength
        public static unsafe IntPtr GetCurLengthAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength;
        }

        // get the curLength value, result = *(memAddr + fullLength)
        public static unsafe Int16 GetCurLength(IntPtr memAddr, Int16 fullLength)
        {
            IntPtr curLengthAddr = memAddr + fullLength;
            return MemInt16.Get(ref curLengthAddr);
        }

        // set the curLength value, *(memAddr + fullLength) = curLength
        public static unsafe void SetCurLength(IntPtr memAddr, Int16 fullLength, Int16 curLength)
        {
            IntPtr curLengthAddr = memAddr + fullLength;
            MemInt16.Set(ref curLengthAddr, curLength);
        }

        // get nextOffset addr, result = memAddr + fullLength + sizeof(curLength)
        public static unsafe IntPtr GetNextOffsetAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength + sizeof(Int16);
        }

        // get the nextOffset value, result = *(memAddr + fullLength + sizeof(curLength))
        public static unsafe Int32 GetNextOffset(IntPtr memAddr, Int16 fullLength)
        {
            IntPtr nextOffsetAddr = memAddr + fullLength + sizeof(Int16);
            return MemInt32.Get(ref nextOffsetAddr);
        }

        // set the nextOffset value, *(memAddr + fullLength + sizeof(curLength)) = curLength
        public static unsafe void SetNextOffset(IntPtr memAddr, Int16 fullLength, Int32 nextOffset)
        {
            IntPtr nextOffsetAddr = memAddr + fullLength + sizeof(Int16);
            MemInt32.Set(ref nextOffsetAddr, nextOffset);
        }

        // get the fullLength for string and list, reuslt = (count + gap) * sizeof(T)
        public static unsafe Int16 GetFullLengthWithCount(int count, Int16 gap, int sizeofT)
        {
            return (Int16)((count + gap) * sizeofT);
        }

        // get the total memory length for string or list, result = sizeof(status)+sizeof(fullLength) + fullLength + sizeof(curLength) + sizeof(nextOffset)
        public static unsafe int GetObjectTotalLengthWithCount(int count, Int16 gap, int sizeofT)
        {
            Int16 fullLength = GetFullLengthWithCount(count, gap, sizeofT);
            return GetObjectTotalLength(fullLength);
        }

        // get the total memory length for object, result = sizeof(byte) + sizeof(Int16) + fullLength + sizeof(Int16) + sizeof(Int32)
        public static unsafe int GetObjectTotalLength(Int16 fullLength)
        {
            return sizeof(Byte) + sizeof(Int16) + fullLength + sizeof(Int16) + sizeof(Int32);
        }

        // get the value size of T
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

        /// <summary>
        /// get string content with length
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="length">The length.</param>
        /// <returns>System.String.</returns>
        public static unsafe string GetChars(IntPtr memAddr, Int16 length)
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
        /// insert string content
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="source">The source.</param>
        public static unsafe void SetChars(IntPtr memAddr, string source, int length)
        {
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(source);
            fixed (void* src = &bytes[0])
            {
                MemTool.Memcpy(memAddr.ToPointer(), src, length);
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
        public static string Get(IntPtr memAddr)
        {
            StringBuilder result = new StringBuilder();

            //get status
            byte status = MemByte.Get(ref memAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //get lastAddr/nextOffsetAddr
            Int16 contentLength;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                contentLength = fullLength;
            else if (MemStatus.GetHasNext(status) == true)
            {
                contentLength = fullLength;
                nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
            }
            else
            {
                //not full
                contentLength = MemTool.GetCurLength(memAddr, fullLength);
            }

            //add string into result
            result.Append(MemTool.GetChars(memAddr, contentLength));

            //recursion next part string
            if (MemStatus.GetHasNext(status) == true)
            {
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);
                result.Append(Get(newAddr));
            }

            return result.ToString();
        }

        /// <summary>
        /// insert Entire string, must has enough space
        /// </summary>
        /// <param name="memAddr">The memory addr.</param>
        /// <param name="source">The source.</param>
        /// <param name="defaultGap">The default gap.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public static bool Set(IntPtr memAddr, string source, Block block)
        {
            //get the length of T
            int sizeofT = sizeof(byte);

            //insert status
            if (block.defaultGap != 0)
                MemByte.Set(ref memAddr, (byte)0x00);
            else
                MemByte.Set(ref memAddr, (byte)0x20);

            //insert length with gap
            Int16 fullLength = MemTool.GetFullLengthWithCount(source.Length, block.defaultGap, sizeofT);
            MemInt16.Set(ref memAddr, fullLength);

            //insert string content
            MemTool.SetChars(memAddr, source, source.Length);

            //set curLength
            if (block.defaultGap != 0)
            {
                MemTool.SetCurLength(memAddr, fullLength, (Int16)source.Length);
            }

            return true;
        }

        public static bool Delete(IntPtr memAddr, Block block)
        {
            //backup offsetMemAddr as free begin addr
            IntPtr stringAddrBackup = memAddr;

            //get status
            byte status = MemByte.Get(ref memAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //get nextOffsetAddr
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetHasNext(status) == true)
            {
                nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetObjectTotalLength(fullLength);
            block.InsertFree(stringAddrBackup, totalMemLength);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);
                return Delete(newAddr, block);
            }

            return true;
        }

        public static bool Update(IntPtr memAddr, string value, Block block)
        {
            //get status
            IntPtr statusAddr = memAddr;
            byte status = MemByte.Get(ref memAddr);
            bool hasNext = MemStatus.GetHasNext(status);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

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
            MemTool.SetChars(memAddr, value, setLength);

            if (value.Length > fullLength)
            {
                //get rest string
                string nextValue = value.Substring(fullLength);

                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);

                //recursion update/set
                if (hasNext == true)
                    return Update(newAddr, nextValue, block);
                else
                    Set(newAddr, nextValue, block);
            }
            else
            {
                //get nextOffset addr
                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);

                //remove next part
                if (hasNext == true)
                    return Delete(newAddr, block);
            }

            return true;
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
        public static List<T> Get<T>(IntPtr memAddr, StructureHelper structureHelper = null)
        {
            List<T> result = new List<T>();

            //get status
            byte status = MemByte.Get(ref memAddr);

            //get length
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //get lastAddr/nextOffsetAddr
            IntPtr lastAddr;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                lastAddr = memAddr + fullLength;
            else if (MemStatus.GetHasNext(status) == true)
            {
                lastAddr = memAddr + fullLength;
                nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
            }
            else
            {
                //not full
                Int16 curLength = MemTool.GetCurLength(memAddr, fullLength);
                lastAddr = memAddr + curLength;
            }

            //add item into list
            while (memAddr.ToInt64() < lastAddr.ToInt64())
            {
                if (typeof(T) == typeof(byte))
                    (result as List<byte>).Add(MemByte.Get(ref memAddr));
                else if (typeof(T) == typeof(Int16))
                    (result as List<Int16>).Add(MemInt16.Get(ref memAddr));
                else if (typeof(T) == typeof(Int32))
                    (result as List<Int32>).Add(MemInt32.Get(ref memAddr));
                else if (typeof(T) == typeof(Int64))
                    (result as List<Int64>).Add(MemInt64.Get(ref memAddr));
                else if (typeof(T) == typeof(String))
                {
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(memAddr);
                    MemInt32.Jump(ref memAddr);
                    (result as List<String>).Add(MemString.Get(newAddr));
                }
                else
                {
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(memAddr);
                    MemInt32.Jump(ref memAddr);
                    (result as List<Structure>).Add(structureHelper.Get(newAddr));
                }
            }

            //recursion next part list
            if (MemStatus.GetHasNext(status) == true)
            {
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);
                result.AddRange(Get<T>(newAddr, structureHelper));
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
        public static bool Set<T>(IntPtr memAddr, List<T> source, Block block, StructureHelper structureHelper = null)
        {
            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

            //insert status
            if (block.defaultGap != 0)
                MemByte.Set(ref memAddr, (byte)0x00);
            else
                MemByte.Set(ref memAddr, (byte)0x20);

            //insert length with gap
            Int16 fullLength = MemTool.GetFullLengthWithCount(source.Count, block.defaultGap, sizeofT);
            MemInt16.Set(ref memAddr, fullLength);

            //backup nextFreeInBlock
            IntPtr listAddrBackup = memAddr;

            //insert list part context
            for (int i = 0; i < source.Count; i++)
            {
                if (typeof(T) == typeof(byte))
                    MemByte.Set(ref memAddr, (source as List<byte>)[i]);
                else if (typeof(T) == typeof(Int16))
                    MemInt16.Set(ref memAddr, (source as List<Int16>)[i]);
                else if (typeof(T) == typeof(Int32))
                    MemInt32.Set(ref memAddr, (source as List<Int32>)[i]);
                else if (typeof(T) == typeof(Int64))
                    MemInt64.Set(ref memAddr, (source as List<Int64>)[i]);
                else if (typeof(T) == typeof(String))
                {
                    IntPtr newAddr = IntPtr.Zero;
                    if (block.GetNewSpace(ref memAddr, (source[i] as string).Length, out newAddr) == false)
                        return false;
                    MemString.Set(newAddr, (source[i] as string), block);
                }
                else
                {
                    IntPtr newAddr = IntPtr.Zero;
                    if (block.GetNewSpace(ref memAddr, (source[i] as string).Length, out newAddr) == false)
                        return false;
                    structureHelper.Set(memAddr, (source[i] as Structure), block);
                }
            }

            //insert curLength
            if (block.defaultGap != 0)
            {
                Int16 curLength = (Int16)(memAddr.ToInt64() - listAddrBackup.ToInt64());
                MemTool.SetCurLength(listAddrBackup, fullLength, curLength);
            }

            return true;
        }

        public static bool Delete<T>(IntPtr memAddr, Block block, StructureHelper structureHelper = null)
        {
            //backup offsetMemAddr as free begin addr 
            IntPtr listAddrBackup = memAddr;

            //get status
            byte status = MemByte.Get(ref memAddr);

            //get fullLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //get lastAddr/nextOffsetAddr
            IntPtr lastAddr;
            IntPtr nextOffsetAddr = IntPtr.Zero;
            if (MemStatus.GetIsFull(status) == true)
                lastAddr = memAddr + fullLength;
            else
            {
                //not full
                Int16 curLength = MemTool.GetCurLength(memAddr, fullLength);
                lastAddr = memAddr + curLength;
            }

            if (MemStatus.GetHasNext(status) == true)
                nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);

            //delete items
            if (typeof(T) == typeof(String))
            {
                while (memAddr.ToInt64() < lastAddr.ToInt64())
                {
                    if (MemString.Delete(memAddr, block) == false)
                        return false;
                }
            }
            else
            {
                while (memAddr.ToInt64() < lastAddr.ToInt64())
                {
                    if (structureHelper.Delete(memAddr, block) == false)
                        return false;
                }
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetObjectTotalLength(fullLength);
            block.InsertFree(listAddrBackup, totalMemLength);

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return Delete<T>(nextOffsetAddr, block, structureHelper);
            }

            return true;
        }

        public static bool Add<T>(IntPtr memAddr, T item, Block block, StructureHelper structureHelper = null)
        {
            //get status
            IntPtr statusAddr = memAddr;
            byte status = MemByte.Get(ref memAddr);

            //get list byteLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //add content
            if (MemStatus.GetIsFull(status) == true)
            {
                if (MemStatus.GetHasNext(status) == true)
                {
                    //get addr to recursion
                    IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);

                    return Add<T>(newAddr, item, block, structureHelper);
                }
                else
                {
                    //update status hasNext
                    MemStatus.SetHasNext(statusAddr, true);

                    //get addr to insert new list
                    IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);

                    //generate new list<T> source
                    List<T> inputs = new List<T>() { item };

                    //insert new list into nextOffset
                    return Set<T>(newAddr, inputs, block, structureHelper);
                }
            }
            else
            {
                IntPtr curLengthAddr = MemTool.GetCurLengthAddr(memAddr, fullLength);
                Int16 curLength = MemTool.GetCurLength(memAddr, fullLength);
                IntPtr insertAddr = memAddr + curLength;

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
                    IntPtr newAddr = IntPtr.Zero;
                    if (block.GetNewSpace(ref insertAddr, (item as string).Length, out newAddr) == false)
                        return false;
                    if (MemString.Set(newAddr, item as string, block) == false)
                        return false;
                }
                else
                {
                    IntPtr newAddr = IntPtr.Zero;
                    if (block.GetNewSpace(ref insertAddr, (item as Structure).Length, out newAddr) == false)
                        return false;
                    if (structureHelper.Set(newAddr, item as Structure, block) == false)
                        return false;
                }

                //update isFull
                if (memAddr + fullLength == insertAddr)
                {
                    MemStatus.SetIsFull(statusAddr, true);
                }
            }

            return true;
        }

        public static unsafe bool Remove<T>(IntPtr memAddr, int index, Block block, StructureHelper structureHelper = null)
        {
            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

            //get status
            IntPtr statusAddr = memAddr;
            byte status = MemByte.Get(ref memAddr);

            //get list byteLength
            Int16 fullLength = MemInt16.Get(ref memAddr);

            //get the distance of index item
            int indexDistance = sizeofT * index;

            //get the real length of body
            Int16 bodyLength = MemStatus.GetIsFull(status) ? fullLength : MemTool.GetCurLength(memAddr, fullLength);

            if (bodyLength > indexDistance)
            {
                //remove the item
                IntPtr removeAddr = memAddr + indexDistance;
                if (typeof(T).IsValueType == true)
                    removeAddr += sizeofT;
                else if (typeof(T) == typeof(String))
                {
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(removeAddr);
                    if (MemString.Delete(newAddr, block) == false)
                        return false;
                }
                else
                {
                    IntPtr newAddr = MemTool.GetAddrByOffsetAddr(removeAddr);
                    if (structureHelper.Delete(newAddr, block) == false)
                        return false;
                }

                //move other items forward
                MemTool.Memcpy(memAddr.ToPointer(), removeAddr.ToPointer(), bodyLength);

                //update curLength
                IntPtr curLengthAddr = MemTool.GetCurLengthAddr(memAddr, fullLength);
                MemInt16.Set(ref curLengthAddr, (Int16)(bodyLength - sizeofT));

                //set isFull=false
                if (MemStatus.GetIsFull(status) == true)
                    MemStatus.SetIsFull(statusAddr, false);
            }
            else
            {
                int nextIndex = index - bodyLength / sizeofT;

                IntPtr nextOffsetAddr = MemTool.GetNextOffsetAddr(memAddr, fullLength);
                IntPtr newAddr = MemTool.GetAddrByOffsetAddr(nextOffsetAddr);

                return Remove<T>(newAddr, nextIndex, block, structureHelper);
            }

            return true;
        }
    }

    public class Structure : IComparable
    {
        public int Length;

        //IComparable
        public virtual int CompareTo(object obj)
        {
            return 0;
        }
    }

    //user defined structure operation
    public interface StructureHelper
    {
        //get with addr
        Structure Get(IntPtr memAddr, int[] headerIndexs = null);

        //set with addr
        bool Set(IntPtr memAddr, Structure source, Block block, int[] headerIndexs = null);

        //delete with addr
        bool Delete(IntPtr memAddr, Block block);
    }
}
