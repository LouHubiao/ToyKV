using System;
using System.Collections.Generic;

namespace ToyGE
{
    class BTreeNode<T>
    {
        //MAX_KEYS = 1024;
        public int isLeaf;  //is this a leaf node?
        public List<T> keys = new List<T>();
        public List<IntPtr> values = new List<IntPtr>();
        public List<BTreeNode<T>> kids = new List<BTreeNode<T>>();
    }

    class Index<T>
    {
        const int MAX_KEYS = 1024;

        //t1>t2, retuen 1; 
        //t1==t2, return 0;
        //t1<t2, return -1;
        public delegate int CompareT(T t1, T t2);
        CompareT compare;

        public delegate T GetDefault();
        GetDefault getDefault;

        public BTreeNode<T> root;

        /// <summary>
        /// init BTree
        /// </summary>
        /// <returns>BTree root</returns>
        public Index(CompareT _compare, GetDefault _getDefault)
        {
            root = new BTreeNode<T>();
            root.isLeaf = 1;
            this.compare = _compare;
            this.getDefault = _getDefault;
        }

        /// <summary>
        /// search in a node
        /// </summary>
        /// <param name="keys">node's keys</param>
        /// <param name="key">target key</param>
        /// <returns>pos in this node</returns>
        public int SearchKey(List<T> keys, T key)
        {
            int lo = -1;
            int hi = keys.Count;
            int mid = -1;
            while (lo + 1 < hi)
            {
                mid = (lo + hi) / 2;
                if (compare(keys[mid], key) == 0)
                {
                    return mid;
                }
                else if (compare(keys[mid], key) < 0)
                {
                    lo = mid;
                }
                else
                {
                    hi = mid;
                }
            }
            return hi;
        }

        /// <summary>
        /// search key in full BTree's subtree
        /// </summary>
        /// <param name="b">root node of subtree</param>
        /// <param name="key">target key</param>
        /// <returns>the value of key</returns>
        public bool BTSearch(BTreeNode<T> b, T key, ref IntPtr result)
        {
            int pos;

            // have to check for empty tree
            if (b.keys.Count == 0)
            {
                return false;
            }

            // look for smallest position that key fits below
            pos = SearchKey(b.keys, key);

            //return the value of key
            if (pos < b.keys.Count && compare(b.keys[pos], key) == 0)
            {
                result = b.values[pos];
                return true;
            }
            else
            {
                if (b.isLeaf == 0)
                {
                    //not found and not leaf, find kid
                    return BTSearch(b.kids[pos], key, ref result);
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// insert core function
        /// </summary>
        /// <param name="b">root node of subtree</param>
        /// <param name="key">insert key</param>
        /// <param name="value">insert value</param>
        /// <param name="medianKey">splie out the mid key</param>
        /// <param name="medianValue">splie out the mid value</param>
        /// <returns>if inserted return null, if splited return right child</returns>
        public BTreeNode<T> BTInsertInternal(BTreeNode<T> b, T key, IntPtr value, ref T medianKey, ref IntPtr medianValue)
        {
            int pos;    //insert pos
            int mid;    //splite pos
            T midKey = getDefault();      //splited mid key
            IntPtr midValue = new IntPtr();    //splited mid value
            BTreeNode<T> b2;

            pos = SearchKey(b.keys, key);

            if (pos < b.keys.Count && compare(b.keys[pos], key) == 0)
            {
                //find nothing to do
                return null;
            }

            if (b.isLeaf == 1)
            {
                /* everybody above pos moves up one space */
                b.keys.Insert(pos, key);
                b.values.Insert(pos, value);
            }
            else
            {
                /* insert in child */
                b2 = BTInsertInternal(b.kids[pos], key, value, ref midKey, ref midValue);

                /* maybe insert a new key in b */
                if (b2 != null)
                {
                    b.keys.Insert(pos, midKey);
                    b.values.Insert(pos, midValue);
                    b.kids.Insert(pos + 1, b2);
                }
            }

            if (b.keys.Count > MAX_KEYS)
            {
                mid = b.keys.Count / 2;

                medianKey = b.keys[mid];
                medianValue = b.values[mid];

                b2 = new BTreeNode<T>();

                b2.isLeaf = b.isLeaf;

                //shallow copy but safe
                int movLen = b.keys.Count - mid - 1;
                b2.keys = b.keys.GetRange(mid + 1, movLen);
                b2.values = b.values.GetRange(mid + 1, movLen);
                b.keys.RemoveRange(mid, movLen + 1);
                b.values.RemoveRange(mid, movLen + 1);
                if (b.isLeaf == 0)
                {
                    //Console.WriteLine(b.kids.Count);
                    b2.kids = b.kids.GetRange(mid + 1, movLen + 1);
                    b.kids.RemoveRange(mid + 1, movLen + 1);
                }

                return b2;
            }

            return null;
        }

        //insert one node into BTree
        public void BTInsert(ref BTreeNode<T> b, T key, IntPtr value)
        {
            BTreeNode<T> b1;   //new left child
            BTreeNode<T> b2;   //new right child
            T medianKey = getDefault();
            IntPtr medianValue = new IntPtr();

            b2 = BTInsertInternal(b, key, value, ref medianKey, ref medianValue);

            // split
            if (b2 != null)
            {
                // root to be child
                b1 = b;

                // make root point to b1 and b2
                b = new BTreeNode<T>();
                b.isLeaf = 0;
                b.keys.Add(medianKey);
                b.values.Add(medianValue);
                b.kids.Add(b1);
                b.kids.Add(b2);
            }
        }
    }
}
