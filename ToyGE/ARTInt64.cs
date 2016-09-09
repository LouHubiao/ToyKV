using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    public class ARTInt64Node
    {
        public IntPtr value = IntPtr.Zero;
        public int prefixLength;
        public Int64 prefix;
        public ARTInt64Node leftChild;
        public ARTInt64Node rightChild;
    }

    public class ARTInt64Tree
    {
        public ARTInt64Node root;
    }

    public class ArtLeaf
    {
        public IntPtr value;

        public ArtLeaf(IntPtr _value)
        {
            this.value = _value;
        }
    }

    public class ARTInt64
    {
        //tree
        public ARTInt64Tree tree;

        public ARTInt64()
        {
            tree = new ARTInt64Tree();
            tree.root = new ARTInt64Node();
        }

        //find from inode to last key item, and than search leaf
        public static bool Search(ARTInt64Tree t, Int64 key, out IntPtr result)
        {
            ARTInt64Node node = t.root;
            if (key == 0)
            {
                return SearchNode(t.root, true, 0, out result);
            }
            else
            {
                return SearchNode(t.root, key % 2 == 0, key >> 1, out result);
            }
        }

        public static bool Insert(ARTInt64Tree t, Int64 key, IntPtr value)
        {
            //0 special
            if (key == 0)
            {
                return InsertNode(t.root, true, 0, value);
            }
            else
            {
                return InsertNode(t.root, key % 2 == 0, key >> 1, value);
            }
        }

        private static bool SearchNode(ARTInt64Node parent, bool isLeft, Int64 key, out IntPtr result)
        {
            ARTInt64Node node;
            Int64 keyCopy = key;
            if (isLeft)
            {
                node = parent.leftChild;
            }
            else
            {
                node = parent.rightChild;
            }

            if (node == null)
            {
                result = IntPtr.Zero;
                return false;
            }

            Int64 prefix = node.prefix;
            int matchCount = 0;
            while (keyCopy != 0 && prefix != 0)
            {
                if (keyCopy % 2 == prefix % 2)
                {
                    keyCopy = keyCopy >> 1;
                    prefix = prefix >> 1;
                    matchCount++;
                }
                else
                {
                    result = IntPtr.Zero;
                    return false;
                }
            }
            if (keyCopy > 0)
            {
                while (node.prefixLength > matchCount)
                {
                    if (keyCopy % 2 == 0)
                    {
                        keyCopy = keyCopy >> 1;
                        matchCount++;
                    }
                    else
                    {
                        result = IntPtr.Zero;
                        return false;
                    }
                }
                return SearchNode(node, keyCopy % 2 == 0, keyCopy >> 1, out result);
            }
            else if (prefix > 0)
            {
                result = IntPtr.Zero;
                return false;
            }
            else if (keyCopy == 0 && prefix == 0)
            {
                result = node.value;
                return true;
            }

            result = IntPtr.Zero;
            return false;
        }

        private static bool InsertNode(ARTInt64Node parent, bool isLeft, Int64 key, IntPtr value)
        {
            //child node
            ARTInt64Node node;
            Int64 keyCopy = key;
            if (isLeft)
            {
                node = parent.leftChild;
            }
            else
            {
                node = parent.rightChild;
            }

            //insert new node into parent if place is null
            if (node == null)
            {
                if (InsertNewNode(parent, isLeft, key, value) == true)
                    return true;
                else if (isLeft)
                    node = parent.leftChild;
                else
                    node = parent.rightChild;
            }

            //compare with prefix
            Int64 prefix = node.prefix;
            int matchCount = 0;
            while (keyCopy != 0 && prefix != 0)
            {
                if (keyCopy % 2 == prefix % 2)
                {
                    //move forward if equals
                    keyCopy = keyCopy >> 1;
                    prefix = prefix >> 1;
                    matchCount++;
                }
                else
                {
                    lock (node)
                    {
                        //not equals, then split node and insert new node
                        if (matchCount == 0)
                            node.prefix = 0;
                        else
                            node.prefix = node.prefix % ((int)Math.Pow(2, matchCount));

                        //inherit parent node's part prefix
                        SplitNode(node, prefix % 2 == 0, prefix >> 1, node.value, node.prefixLength - matchCount - 1);
                        node.prefixLength = matchCount;

                        InsertNewNode(node, keyCopy % 2 == 0, keyCopy >> 1, value);
                        node.value = IntPtr.Zero;
                        return true;
                    }
                }
            }
            if (keyCopy > 0)
            {
                //not finished match, more 0 in prefix
                while (node.prefixLength > matchCount)
                {
                    if (keyCopy % 2 == 0)
                    {
                        keyCopy = keyCopy >> 1;
                        matchCount++;
                    }
                    else
                    {
                        lock (node) {
                            if (matchCount == 0)
                                node.prefix = 0;
                            else
                                node.prefix = node.prefix % ((int)Math.Pow(2, matchCount));

                            //remove prefixLength
                            SplitNode(node, true, 0, node.value, node.prefixLength - matchCount - 1);
                            node.prefixLength = matchCount;

                            InsertNewNode(node, false, keyCopy >> 1, value);
                            return true;
                        }
                    }
                }
                InsertNode(node, keyCopy % 2 == 0, keyCopy >> 1, value);
            }
            else if (prefix > 0)
            {
                //prefix is more than key
                int prefixLength = prefix >> 1 == 0 ? 0 : Convert.ToString(prefix >> 1, 2).Length;
                SplitNode(node, prefix % 2 == 0, prefix >> 1, node.value, prefixLength);
                node.value = value;
                node.prefix = key;
                node.prefixLength = key == 0 ? 0 : Convert.ToString(key, 2).Length;
                if (prefix % 2 == 0)
                    node.rightChild = null;
                else
                    node.leftChild = null;
            }
            else if (keyCopy == 0 && prefix == 0)
            {
                //update
                node.value = value;
            }
            return true;
        }

        private static bool InsertNewNode(ARTInt64Node parent, bool isLeft, Int64 key, IntPtr value)
        {
            bool result = true;
            if (isLeft && parent.leftChild != null)
                result = false;
            else if (!isLeft && parent.rightChild != null)
                result = false;

            ARTInt64Node node = new ARTInt64Node();
            node.prefix = key;
            node.prefixLength = Convert.ToString(key, 2).Length;
            node.value = value;

            //add into parent
            if (isLeft)
            {
                parent.leftChild = node;
            }
            else
            {
                parent.rightChild = node;
            }

            return result;
        }

        private static void SplitNode(ARTInt64Node parent, bool isLeft, Int64 key, IntPtr value, int prefixLen)
        {
            ARTInt64Node node = new ARTInt64Node();
            node.prefix = key;
            node.prefixLength = prefixLen;
            node.value = value;

            //use old points
            if (parent.leftChild != null || parent.rightChild != null)
            {
                node.leftChild = parent.leftChild;
                node.rightChild = parent.rightChild;
            }

            //add into parent
            if (isLeft)
            {
                parent.leftChild = node;
            }
            else
            {
                parent.rightChild = node;
            }
        }
    }
}
