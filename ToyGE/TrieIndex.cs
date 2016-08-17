using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    class TrieNode
    {
        public IntPtr addr = new IntPtr(0);
        public int[] childrenIndexs = new int[62];
    }

    class TrieIndex
    {
        public List<TrieNode> trieNodes;
        int tailIndex = 0;

        public TrieIndex()
        {
            trieNodes = new List<TrieNode>();
        }

        public void Insert(string key, IntPtr value)
        {
            if (key == null)
                return;
            int trieIndex = 0;
            for (int i = 0; i < key.Length; i++)
            {
                char ch = key[i];
                int offset = GetOffset(ch);
                int nextIndex = trieNodes[trieIndex].childrenIndexs[offset];
                if (nextIndex != 0)
                    trieIndex = nextIndex;
                else
                {
                    tailIndex++;
                    trieNodes.Add(new TrieNode());
                    trieNodes[tailIndex].childrenIndexs[offset] = tailIndex;
                    trieIndex = tailIndex;
                }
            }
            trieNodes[trieIndex].addr = new IntPtr(value.ToInt64());
        }

        public IntPtr Search(string key)
        {
            int trieIndex = 0;
            for (int i = 0; i < key.Length; i++)
            {
                //if the last, return addr
                if (i == key.Length - 1)
                {
                    return trieNodes[trieIndex].addr;
                }

                //search next
                char ch = key[i];
                int offset = GetOffset(ch);
                int nextIndex = trieNodes[trieIndex].childrenIndexs[offset];
                if (nextIndex != 0)
                    trieIndex = nextIndex;
                else
                {
                    return new IntPtr(0);
                }
            }
            return new IntPtr(0);
        }

        public int GetOffset(char ch)
        {
            short assic = (short)ch;
            int realOffset = 0;
            if (assic < 58)
                realOffset = assic - 48;
            else if (assic < 91)
                realOffset = assic - 65 + 10;
            else if (assic < 123)
                realOffset = assic - 97 + 36;

            return realOffset;
        }
    }
}
