using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    public class Block<K>
    {
        //freelist max item is 64KB, pre offset is 64
        const int freeCount = 1024;

        public IntPtr headAddr;
        public IntPtr tailAddr;
        public IntPtr[] freeList;
        public int blockLength;
        public B_Tree<K, IntPtr> blockIndex;

        public Block(int blockLength)
        {
            IntPtr memAddr = Marshal.AllocHGlobal(blockLength);

            this.headAddr = memAddr;
            this.tailAddr = memAddr;
            this.freeList = new IntPtr[freeCount];
            this.blockLength = blockLength;
            this.blockIndex = new B_Tree<K, IntPtr>();     //alloc blockIndex
        }


    }
}
