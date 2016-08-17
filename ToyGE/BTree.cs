using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    //MAX_KEYS = 1024;

    public class Node<K, V>
    {
        public int isLeaf;  //is this a leaf node?
        public List<K> keys = new List<K>();
        public List<V> values = new List<V>();
        public List<Node<K, V>> kids = new List<Node<K, V>>();
    }

    public class MedianNode<K, V>
    {
        public K key;
        public V value;
        public Node<K, V> node;
    }

    public class B_Tree<K, V>
    {
        const int MAX_KEYS = 1024;

        //t1>t2, retuen 1; 
        //t1==t2, return 0;
        //t1<t2, return -1;
        //Delegate<K, V>.CompareT compare;
        //Delegate<K, V>.GetDefaultKey getDefaultKey;
        //Delegate<K, V>.GetDefaultValue getDefaultVaule;

        public Node<K, V> root;

        /// <summary>
        /// init BTree
        /// </summary>
        /// <returns>BTree root</returns>
        public B_Tree()
        {
            root = new Node<K, V>();
            root.isLeaf = 1;
        }

        /// <summary>
        /// search key in full BTree's subtree
        /// </summary>
        /// <param name="root">root node of subtree</param>
        /// <param name="key">target key</param>
        /// <returns>the value of key</returns>
        public static bool Search(Node<K, V> root, K key, Delegate<K>.CompareT compare, out V node)
        {
            int pos;

            // have to check for empty tree
            if (root.keys.Count == 0)
            {
                node = default(V);
                return false;
            }

            // look for smallest position that key fits below
            pos = SearchKeyInNode(root.keys, key, compare);

            //return the value of key
            if (pos < root.keys.Count && compare(root.keys[pos], key) == 0)
            {
                node = root.values[pos];
                return true;
            }
            else
            {
                if (root.isLeaf == 0)
                {
                    //not found and not leaf, find kid
                    return Search(root.kids[pos], key, compare, out node);
                }
                else
                {
                    node = default(V);
                    return false;
                }
            }
        }

        //insert one node into BTree
        public static void Insert(ref Node<K, V> b, K key, V value, Delegate<K>.CompareT compare)
        {
            Node<K, V> b1;   //new left child
            MedianNode<K, V> b2;   //new right child

            b2 = BTInsertInternal(b, key, value, compare);

            // split
            if (b2 != null)
            {
                // root to be child
                b1 = b;

                // make root point to b1 and b2
                b = new Node<K, V>();
                b.isLeaf = 0;
                b.keys.Add(b2.key);
                b.values.Add(b2.value);
                b.kids.Add(b1);
                b.kids.Add(b2.node);
            }
        }

        /// <summary>
        /// search in a node by binary search
        /// </summary>
        /// <param name="keys">node's keys</param>
        /// <param name="key">target key</param>
        /// <returns>pos in this node</returns>
        static int SearchKeyInNode(List<K> keys, K key, Delegate<K>.CompareT compare)
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
        /// insert core function
        /// </summary>
        /// <param name="b">root node of subtree</param>
        /// <param name="key">insert key</param>
        /// <param name="value">insert value</param>
        /// <param name="medianKey">splie out the mid key</param>
        /// <param name="medianValue">splie out the mid value</param>
        /// <returns>if inserted return null, if splited return right child</returns>
        static MedianNode<K, V> BTInsertInternal(Node<K, V> b, K key, V value, Delegate<K>.CompareT compare)
        {
            int pos;    //insert pos
            int mid;    //splite pos
            MedianNode<K, V> b2;

            pos = SearchKeyInNode(b.keys, key, compare);

            if (b.isLeaf == 1)
            {
                /* everybody above pos moves up one space */
                b.keys.Insert(pos, key);
                b.values.Insert(pos, value);
            }
            else
            {
                /* insert in child */
                b2 = BTInsertInternal(b.kids[pos], key, value, compare);

                /* maybe insert a new key in b */
                if (b2 != null)
                {
                    b.keys.Insert(pos, b2.key);
                    b.values.Insert(pos, b2.value);
                    b.kids.Insert(pos + 1, b2.node);
                }
            }

            if (b.keys.Count > MAX_KEYS)
            {
                mid = b.keys.Count / 2;

                b2 = new MedianNode<K, V>();
                b2.key = b.keys[mid];
                b2.value = b.values[mid];
                b2.node = new Node<K, V>();

                b2.node.isLeaf = b.isLeaf;
                //shallow copy but safe
                int movLen = b.keys.Count - mid - 1;
                b2.node.keys = b.keys.GetRange(mid + 1, movLen);
                b2.node.values = b.values.GetRange(mid + 1, movLen);
                b.keys.RemoveRange(mid, movLen + 1);
                b.values.RemoveRange(mid, movLen + 1);
                if (b.isLeaf == 0)
                {
                    b2.node.kids = b.kids.GetRange(mid + 1, movLen + 1);
                    b.kids.RemoveRange(mid + 1, movLen + 1);
                }
                return b2;
            }

            return null;
        }
    }
}
