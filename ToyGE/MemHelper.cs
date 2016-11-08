using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

/*
memory struct:
    string: status(8)| fullLength(16)| Body| [CurLength(16)]| [NextOffset(32)]|
    list:   status(8)| fullLength(16)| Body| [CurLength(16)]| [NextOffset(32)]|
    struct: status(8)| Body|

status:
    string: hasNext| isFull| ...
    list:   hasNext| isFull| ...
    struct: ...

ps:
1.fullLength is the length of Body, curLength is the length of content
*/

namespace ToyGE
{
    /// <summary>
    /// the usual tool about memory management of different structure
    /// </summary>
    class MemTool
    {
        /// <summary>
        /// Get next address by offset, result = memAddr + sizeof(Int32) + *(memAddr)
        /// </summary>
        /// <param name="memAddr">Addr Before Offset</param>
        /// <returns>offseted address</returns>
        public static unsafe IntPtr GetAddrByAddrBeforeOffset(ref IntPtr memAddr)
        {
            Int32 offset = MemInt32.Get(ref memAddr);
            //memAddrBeforeOffset has been changed
            return memAddr + offset;
        }

        public static unsafe IntPtr GetCurLengthAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength;
        }

        /// <summary>
        /// get the curLength, result = *(memAddr + fullLength)
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
        /// get addr before nextOffset
        /// </summary>
        /// <param name="memAddr">memAddr after fullLength</param>
        /// <param name="fullLength"></param>
        /// <returns></returns>
        public static unsafe IntPtr GetNextOffsetAddr(IntPtr memAddr, Int16 fullLength)
        {
            return memAddr + fullLength + sizeof(Int16);
        }

        /// <summary>
        /// get the nextOffset, result = *(memAddr + fullLength + sizeof(Int16))
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
        /// set the curLength value, *(memAddr + fullLength + sizeof(Int16)) = curLength
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
        /// jump some interval
        /// </summary>
        /// <param name="memAddr"></param>
        /// <param name="interval"></param>
        public static unsafe void addrJump(ref IntPtr memAddr, int interval)
        {
            memAddr += interval;
        }

        /// <summary>
        /// get the fullLength for string and list, reuslt = (count+gap)*sizeof(T)
        /// </summary>
        /// <typeparam name="T">byte or type in list</typeparam>
        /// <param name="count"></param>
        /// <param name="gap"></param>
        /// <returns></returns>
        public static unsafe Int16 GetFullLength<T>(int count, Int16 gap)
        {
            if (typeof(T).IsValueType)
                return (Int16)((count + gap) * Marshal.SizeOf<T>());
            else
                return (Int16)((count + gap) * sizeof(Int32));
        }

        /// <summary>
        /// get structure Length for string or list, result = sizeof(status)+sizeof(fullLength) + fullLength + sizeof(curLength) + sizeof(nextOffset)
        /// </summary>
        /// <typeparam name="T">byte or type in list</typeparam>
        /// <param name="count"></param>
        /// <param name="gap"></param>
        /// <returns></returns>
        public static unsafe int GetNeedMemlength<T>(int count, Int16 gap)
        {
            int result = sizeof(Byte) + sizeof(Int16);
            Int16 fullLength = GetFullLength<T>(count, gap);
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
    class MemByte
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
    class MemInt16
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
    class MemInt32
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
    class MemInt64
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
    class MemStatus
    {
        public static bool GetIsDeleted(byte status)
        {
            byte mask = 0x80;
            byte isDeleted = (byte)(status & mask);
            return isDeleted > 0;
        }

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
    class MemString
    {
        //get entire string
        public static string Get(ref IntPtr memAddr)
        {
            //get string values begin addr
            IntPtr offsetMemAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            byte status = MemByte.Get(ref offsetMemAddr);

            //get length
            Int16 length = MemInt16.Get(ref offsetMemAddr);

            //get context
            if (MemStatus.GetIsFull(status) == true)
            {
                //full
                return GetChars(ref offsetMemAddr, length);
            }
            else if (MemStatus.GetHasNext(status) == true)
            {
                //has next
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.Append(GetChars(ref offsetMemAddr, length - sizeof(Int32)));

                IntPtr nextPartAddr = MemTool.GetAddrByAddrBeforeOffset(ref offsetMemAddr);
                strBuilder.Append(Get(ref nextPartAddr));

                return strBuilder.ToString();
            }
            else
            {
                //not full
                Int16 curCount = MemTool.GetCurCount(offsetMemAddr, length);
                return GetChars(ref offsetMemAddr, curCount);
            }
        }

        //insert Entire string, must has enough space
        public static bool Set(ref IntPtr memAddr, string content, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap)
        {
            //get nextFreeAddr in this block
            Int16 byteLength = MemTool.GetNeedMemlength<byte>((Int16)content.Length, gap);
            Int16 fullLength = (Int16)(sizeof(byte) + sizeof(Int16) + byteLength);
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(freeList, headAddr, ref tailAddr, blockLength, fullLength);
            if (nextFreeInBlock.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(nextFreeInBlock.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            if (gap != 0)
            {
                //insert status
                MemByte.Set(ref nextFreeInBlock, (byte)0x0);

                //insert length
                MemInt16.Set(ref nextFreeInBlock, byteLength);

                //lastAddr buffer
                IntPtr lastAddr = nextFreeInBlock + byteLength;

                //insert curLength
                MemTool.SetCurCount(nextFreeInBlock, byteLength, (Int16)(content.Length));

                //inser content
                InsertChars(ref nextFreeInBlock, content);

                //nextPartAddr jump to last
                nextFreeInBlock = lastAddr;
            }
            else
            {
                //insert status, isFull=1
                MemByte.Set(ref nextFreeInBlock, (byte)0x20);

                //insert length
                MemInt16.Set(ref nextFreeInBlock, (Int16)content.Length);

                //inser content
                InsertChars(ref nextFreeInBlock, content);
            }

            return true;
        }

        //delete Entire string
        public static unsafe void Delete(ref IntPtr memAddr, IntPtr[] freeList)
        {
            IntPtr memAddrCopy = memAddr;

            //get offseted addr
            IntPtr offsetMemAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //delete offset pointer
            *(Int32*)(memAddrCopy.ToPointer()) = 0;

            //backup offsetMemAddr as free begin addr 
            IntPtr contentBeginAddr = offsetMemAddr;

            //change status isDelete=1
            byte* status = (byte*)(offsetMemAddr.ToPointer());
            MemStatus.SetIsDeleted(status, true);
            offsetMemAddr += 1;

            //get string length: byte count
            Int16 length = MemInt16.Get(ref offsetMemAddr);

            //if has next part, recursion
            if (MemStatus.GetHasNext(*status) == true)
            {
                IntPtr nextPartOffset = MemTool.GetNextOffsetAddr(offsetMemAddr, length);
                Delete(ref nextPartOffset, freeList);
            }

            //add to freeAddr[length]
            if (length >= 64)
            {
                //insert content into freeAddr
                MemFreeList.InsertFreeList(contentBeginAddr, length, freeList);

                //merge with after
                IntPtr nextAddr = offsetMemAddr + length;
                MemFreeList.MergeWithNext(contentBeginAddr, nextAddr, freeList);
            }
            //else drop and wait to auto GC
        }

        //update string, return false is no free space, return true is update success
        public static unsafe bool Update(ref IntPtr memAddr, ref string newValue, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap)
        {
            //get offseted addr
            IntPtr offsetMemAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            byte* status = (byte*)(offsetMemAddr.ToPointer());

            //go to next pointer
            offsetMemAddr += 1;

            //get length
            Int16 length = MemInt16.Get(ref offsetMemAddr);

            //update content
            if (MemStatus.GetHasNext(*status) == true)
            {
                if (length > newValue.Length)
                {
                    //delete nextPart
                    IntPtr nextPartAddr = MemTool.GetNextOffsetAddr(offsetMemAddr, length);
                    Delete(ref nextPartAddr, freeList);

                    //update status, hasNext=0, isFull=0
                    MemStatus.SetHasNext(status, false);
                    MemStatus.SetIsFull(status, false);

                    //insert curLength
                    MemTool.SetCurCount(offsetMemAddr, length, (Int16)newValue.Length);

                    //insert content
                    InsertChars(ref offsetMemAddr, newValue);
                }
                else if (length == newValue.Length)
                {
                    //delete nextPart
                    IntPtr nextPartAddr = MemTool.GetNextOffsetAddr(offsetMemAddr, length);
                    Delete(ref nextPartAddr, freeList);

                    //insert status, hasNext=0, isFull=1
                    MemStatus.SetHasNext(status, false);
                    MemStatus.SetIsFull(status, true);

                    //insert content
                    InsertChars(ref offsetMemAddr, newValue);
                }
                else
                {
                    //get nextPart offset addr
                    IntPtr nextPartAddr = MemTool.GetNextOffsetAddr(offsetMemAddr, length);

                    //insert content, left into orig
                    string leftString = newValue.Substring(0, length - sizeof(Int32));
                    InsertChars(ref offsetMemAddr, leftString);

                    //insert content, right into next, return false if not success (part inserted)
                    newValue = newValue.Substring(length - sizeof(Int32));
                    if (Update(ref nextPartAddr, ref newValue, freeList, headAddr, ref tailAddr, blockLength, gap) == false)
                        return false;
                }
            }
            else
            {
                if (length > newValue.Length)
                {
                    //update status, isFull=0
                    MemStatus.SetIsFull(status, false);

                    //insert curCount
                    MemTool.SetCurCount(offsetMemAddr, length, (Int16)newValue.Length);

                    //insert content
                    InsertChars(ref offsetMemAddr, newValue);
                }
                else if (length == newValue.Length)
                {
                    //insert status, isFull=1
                    MemStatus.SetIsFull(status, true);

                    //insert content
                    InsertChars(ref offsetMemAddr, newValue);
                }
                else
                {
                    //update status, isFull=0
                    MemStatus.SetIsFull(status, false);

                    //get left string into orig memory
                    string leftString = newValue.Substring(0, length - sizeof(Int32));

                    //insert temp curLength
                    MemTool.SetCurCount(offsetMemAddr, length, (Int16)(length - sizeof(Int32)));

                    //insert left string
                    InsertChars(ref offsetMemAddr, leftString);

                    //get right string into new next free
                    newValue = newValue.Substring(length - sizeof(Int32));

                    //insert right string first, if failed, return 0 and chenge isFull
                    if (Set(ref offsetMemAddr, newValue, freeList, headAddr, ref tailAddr, blockLength, gap) == false)
                        return false;

                    //update status, hasNext=1
                    MemStatus.SetHasNext(status, true);
                }
            }
            return true;
        }

        public static unsafe void Jump(ref IntPtr memAddr)
        {
            memAddr += sizeof(Int32);
        }

        //get string content
        static unsafe string GetChars(ref IntPtr memAddr, int length)
        {
            if (length > 0)
            {
                byte[] resultBytes = new byte[length];
                fixed (void* dest = &resultBytes[0])
                {
                    MemTool.Memcpy(dest, memAddr.ToPointer(), length);
                }
                string result = System.Text.Encoding.ASCII.GetString(resultBytes);
                memAddr += length;
                return result;
            }
            return string.Empty;
        }

        //isnert string content
        static unsafe void InsertChars(ref IntPtr memAddr, string input)
        {
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(input);
            fixed (void* source = &bytes[0])
            {
                MemTool.Memcpy(memAddr.ToPointer(), source, bytes.Length);
            }
            memAddr += bytes.Length;
        }
    }

    //list operation
    class MemList
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
        /// <param name="inputs"></param>
        /// <param name="freeList"></param>
        /// <param name="headAddr"></param>
        /// <param name="tailAddr"></param>
        /// <param name="blockLength"></param>
        /// <param name="defaultGap"></param>
        /// <param name="insertItem_Object"></param>
        /// <param name="insertItem_Value"></param>
        /// <returns></returns>
        public static bool Set<T>(ref IntPtr memAddr, List<T> inputs, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap, Delegate<T>.InsertItem_Object insertItem_Object, Delegate<T>.InsertItem_Value insertItem_Value)
        {
            //get nextFreeAddr in this block, only for list, maybe not enough for all items
            int memLength = MemTool.GetNeedMemlength<T>(inputs.Count, defaultGap);
            IntPtr ListAddr = MemFreeList.GetFreeInBlock(freeList, headAddr, ref tailAddr, blockLength, memLength);
            if (ListAddr.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(ListAddr.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //insert status
            if (defaultGap != 0)
                MemByte.Set(ref ListAddr, (byte)0x00);
            else
                MemByte.Set(ref ListAddr, (byte)0x20);

            //insert length with gap
            Int16 fullLength = MemTool.GetFullLength<T>(inputs.Count, defaultGap);
            MemInt16.Set(ref ListAddr, fullLength);

            //backup nextFreeInBlock
            IntPtr nextFreeInBlockCopy = ListAddr;

            //insert list part context
            if (typeof(T).IsValueType)
            {
                for (int i = 0; i < inputs.Count; i++)
                {
                    if (insertItem_Value(ref ListAddr, inputs[i]) == false)
                        return false;
                }
            }
            else
            {
                for (int i = 0; i < inputs.Count; i++)
                {
                    if (insertItem_Object(ref ListAddr, inputs[i], freeList, headAddr, ref tailAddr, blockLength, defaultGap) == false)
                        return false;
                }
            }

            //insert curLength
            if (defaultGap != 0)
            {
                Int16 curLength = (Int16)(ListAddr.ToInt64() - nextFreeInBlockCopy.ToInt64());
                MemTool.SetCurLength(nextFreeInBlockCopy, fullLength, curLength);
            }

            return true;
        }

        /// <summary>
        /// deelte list content, return false if failed
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memAddr"></param>
        /// <param name="freeList"></param>
        /// <param name="deleteItem_Object"></param>
        public static bool Delete<T>(ref IntPtr memAddr, IntPtr[] freeList, Delegate<T>.DeleteItem_Object deleteItem_Object)
        {
            //get offseted addr
            IntPtr listAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //backup offsetMemAddr as free begin addr 
            IntPtr ListAddrBackup = listAddr;

            //change status isDelete=1
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

            //recursion delete
            if (MemStatus.GetHasNext(status) == true)
            {
                return MemList.Delete<T>(ref nextOffsetAddr, freeList, deleteItem_Object);
            }

            //insert content into freeAddr
            int totalMemLength = MemTool.GetToalMemLengthbyFullLength(fullLength);
            MemFreeList.InsertFreeList(ListAddrBackup, totalMemLength, freeList);

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
        public unsafe static bool Add<T>(IntPtr memAddr, T item, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 defaultGap, Delegate<T>.InsertItem_Object insertItem_Object, Delegate<T>.InsertItem_Value insertItem_Value)
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

                    //generate new list<T> inputs
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
                    if (insertItem_Value(ref insertAddr, item) == false)
                        return false;
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
            //get offseted addr
            IntPtr listAddr = MemTool.GetAddrByAddrBeforeOffset(ref memAddr);

            //get status
            IntPtr statusAddr = listAddr;
            byte status = MemByte.Get(ref listAddr);

            //get list byteLength
            Int16 fullLength = MemInt16.Get(ref listAddr);

            //get the length of T
            int sizeofT = typeof(T).IsValueType ? MemTool.GetValueTypeSize(typeof(T)) : sizeof(Int32);

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

        ////remove an item by index from list
        //public static unsafe void Jump(ref IntPtr memAddr)
        //{
        //    memAddr += sizeof(Int32);
        //}
    }

    //free memory operation
    class MemFreeList
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

    //cell operation
    class MemCell
    {
        //update cell(updatingAddr) nextNode and preNode
        public static unsafe void UpdateNextNode_PreNode(IntPtr updatingAddr, IntPtr preAddr)
        {
            if (preAddr.ToInt64() != 0)
            {
                Int32* pre_nextNode = (Int32*)(preAddr + 1);
                *pre_nextNode = (Int32)(updatingAddr.ToInt64() - preAddr.ToInt64());

                Int32* cur_preNode = (Int32*)(updatingAddr + 5);
                *cur_preNode = (Int32)(updatingAddr.ToInt64() - preAddr.ToInt64());
            }
            else
            {
                Int32* cur_preNode = (Int32*)(updatingAddr + 5);
                *cur_preNode = (Int32)0;
            }
        }
    }
}
