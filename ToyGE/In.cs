using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using System.Collections;

namespace ToyGE
{
    public class In : Structure
    {
        public new int Length = 13;

        [JsonProperty("addr")]
        public string Addr;

        [JsonProperty("tx_index")]
        public Int64 Tx_index;

        //IComparable
        public override int CompareTo(object obj)
        {
            In another = obj as In;
            if (this.Addr == another.Addr && this.Tx_index == another.Tx_index)
                return 0;
            else
                return this.Addr.CompareTo(another.Addr);
        }
    }

    public class InHelper : StructureHelper
    {
        public enum Header
        {
            Addr = 1,
            Tx_index = 2
        }

        public Structure Get(IntPtr memAddr, int[] headerIndexs = null)
        {
            In result = new In();

            byte status = MemByte.Get(ref memAddr);

            if (headerIndexs == null || ((IList)headerIndexs).Contains(Header.Addr))
            {
                IntPtr AddrAddr = MemTool.GetAddrByOffsetAddr(memAddr);
                result.Addr = MemString.Get(AddrAddr);
                MemInt32.Jump(ref memAddr);
            }
            else
                MemInt32.Jump(ref memAddr);

            if (headerIndexs == null || ((IList)headerIndexs).Contains(Header.Tx_index))
            {
                result.Tx_index = MemInt64.Get(ref memAddr);
            }
            else
                MemInt64.Jump(ref memAddr);

            return result;
        }

        public bool Set(IntPtr memAddr, Structure source, Block block, int[] headerIndexs = null)
        {
            In value = source as In;

            //insert inStatus
            MemByte.Set(ref memAddr, (byte)0);

            //insert in_addr
            if (headerIndexs == null || ((IList)headerIndexs).Contains(Header.Addr))
            {
                IntPtr newAddr = IntPtr.Zero;
                if (block.GetNewSpace(ref memAddr, value.Addr.Length, out newAddr) == false)
                    return false;
                MemString.Set(newAddr, value.Addr, block);
            }
            else
                MemInt32.Jump(ref memAddr);

            //insert tx_index
            if (headerIndexs == null || ((IList)headerIndexs).Contains(Header.Tx_index))
                MemInt64.Set(ref memAddr, value.Tx_index);
            else
                MemInt64.Jump(ref memAddr);

            return true;
        }

        public bool Delete(IntPtr memAddr, Block block)
        {
            //backup memAddr 
            IntPtr memAddrBackup = memAddr;

            //get status
            byte status = MemByte.Get(ref memAddr);

            if (MemString.Delete(memAddr, block) == false)
                return false;   
            MemInt32.Jump(ref memAddr);

            block.InsertFree(memAddrBackup, 13);

            return true;
        }
    }
}
