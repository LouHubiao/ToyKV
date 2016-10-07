using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace ToyGE
{
    public class In:IComparable
    {
        [JsonProperty("addr")]
        public string addr;

        [JsonProperty("tx_index")]
        public Int64 tx_index;

        public int CompareTo(object obj)
        {
            In another = obj as In;
            if (this.addr == another.addr && this.tx_index == another.tx_index)
                return 0;
            else
                return this.addr.CompareTo(another.addr);
        }
    }

    public class InHelper
    {
        /// <summary>
        /// get struct part [In] of object
        /// </summary>
        /// <param name="structAddr">part begin address</param>
        /// <returns>struct part object</returns>
        public static In Get(ref IntPtr structAddr)
        {
            In result = new In();

            IntPtr offsetMemAddr = MemTool.GetOffsetedAddr(ref structAddr);

            byte status = MemByte.Get(ref offsetMemAddr);

            result.addr = MemString.Get(ref offsetMemAddr);

            result.tx_index = MemInt64.Get(ref offsetMemAddr);

            return result;
        }

        /// <summary>
        /// set struct part [In] of object, need many paras which get by cell set, must follow by cell set
        /// </summary>
        /// <param name="memAddr">next Free In Block</param>
        /// <param name="value">insert object</param>
        /// <param name="freeList">freelist of different size space</param>
        /// <param name="headAddr">block head addr</param>
        /// <param name="tailAddr">cur tail addr</param>
        /// <param name="blockLength">block size</param>
        /// <param name="gap"></param>
        /// <returns></returns>
        public static bool Set(ref IntPtr memAddr, In value, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap)
        {
            //judge if has enough space for struct (space=13, get before compile)
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(freeList, headAddr, ref tailAddr, blockLength, 13);
            if (nextFreeInBlock.ToInt64() == 0)
                return false;   //set false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(nextFreeInBlock.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //insert inStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert in_addr
            MemString.Set(ref nextFreeInBlock, value.addr, freeList, headAddr, ref tailAddr, blockLength, gap);

            //insert tx_index
            MemInt64.Set(ref nextFreeInBlock, value.tx_index);

            return true;
        }
    }
}
