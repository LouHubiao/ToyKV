using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ToyGE
{
    //MAX KEYS in one node is 1024;

    class Node<K>
    {
        public int isLeaf;
        public List<K> keys = new List<K>();
        public List<IntPtr> values = new List<IntPtr>();
        public List<Node<K>> kids = new List<Node<K>>();
    }

    class NewValueInNode<K>
    {
        public K key;
        public IntPtr value;
        public Node<K> node;
    }

    /// <summary>
    /// Class BTree.
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <seealso cref="ToyGE.Index{K}" />
    public class BTree<K> : Index<K>
    {
        const int MAX_KEYS = 1024;

        //_root about index tree
        Node<K> _root;

        //t1>t2, return 1; 
        //t1==t2, return 0;
        //t1<t2, return -1;
        Delegate<K>.CompareT _compare;

        /// <summary>
        /// initiation BTree
        /// </summary>
        /// <returns>BTree node</returns>
        public BTree(Delegate<K>.CompareT pCompare)
        {
            _root = new Node<K>();
            _root.isLeaf = 1;
            _compare = pCompare;
        }

        /// <summary>
        /// Search by key in BTree.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="result">The result.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public bool Search(K key, out IntPtr result)
        {
            return SearchInSubTree(_root, key, out result);
        }

        /// <summary>
        /// insert one result into BTree.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Insert(K key, IntPtr value)
        {
            //new right child
            NewValueInNode<K> newRightNode;

            newRightNode = BTreeInsertInternal(_root, key, value);

            // update _root node
            if (newRightNode != null)
            {
                //new left child
                Node<K> rootBackup;

                // node to be child
                rootBackup = _root;

                // make node point to b1 and b2
                _root = new Node<K>();
                _root.isLeaf = 0;
                _root.keys.Add(newRightNode.key);
                _root.values.Add(newRightNode.value);
                _root.kids.Add(rootBackup);
                _root.kids.Add(newRightNode.node);
            }
        }

        /// <summary>
        /// search key in subtree
        /// </summary>
        /// <param name="node">_root of subtree</param>
        /// <param name="key">target key</param>
        /// <param name="result">The result.</param>
        /// <returns>the value of key</returns>
        private bool SearchInSubTree(Node<K> node, K key, out IntPtr result)
        {
            int position;

            // empty tree
            if (node.keys.Count == 0)
            {
                result = IntPtr.Zero;
                return false;
            }

            // look for smallest position that key fits below
            position = SearchKeyInOneNode(node.keys, key);

            //return the value of key
            if (position < node.keys.Count && _compare(node.keys[position], key) == 0)
            {
                result = node.values[position];
                return true;
            }
            else
            {
                if (node.isLeaf == 0)
                {
                    //not found and not leaf, find kid
                    return SearchInSubTree(node.kids[position], key, out result);
                }
                else
                {
                    result = IntPtr.Zero;
                    return false;
                }
            }
        }

        /// <summary>
        /// search in one node by binary search
        /// </summary>
        /// <param name="keys">result's keys</param>
        /// <param name="key">target key</param>
        /// <returns>pos in this result</returns>
        private int SearchKeyInOneNode(List<K> keys, K key)
        {
            if (keys == null || keys.Count == 0)
                return 0;

            int lo = 0;
            int hi = keys.Count - 1;
            int mid = 0;
            while (lo <= hi)
            {
                mid = (lo + hi) / 2;
                if (_compare(keys[mid], key) == 0)
                {
                    return mid;
                }
                else if (_compare(keys[mid], key) < 0)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return lo;
        }

        /// <summary>
        /// insert core function
        /// </summary>
        /// <param name="node">node result of subtree</param>
        /// <param name="key">insert key</param>
        /// <param name="value">insert value</param>
        /// <returns>if inserted return null, if split return right child</returns>
        private NewValueInNode<K> BTreeInsertInternal(Node<K> node, K key, IntPtr value)
        {
            int position;    //insert pos
            //new value in father node
            NewValueInNode<K> newValueInNode;

            position = SearchKeyInOneNode(node.keys, key);

            if (node.isLeaf == 1)
            {
                //insert into leaf
                node.keys.Insert(position, key);
                node.values.Insert(position, value);
            }
            else
            {
                //recursion insert
                newValueInNode = BTreeInsertInternal(node.kids[position], key, value);

                //insert into father node
                if (newValueInNode != null)
                {
                    node.keys.Insert(position, newValueInNode.key);
                    node.values.Insert(position, newValueInNode.value);
                    node.kids.Insert(position + 1, newValueInNode.node);
                }
            }

            //split
            if (node.keys.Count > MAX_KEYS)
            {
                //split position
                int mid = node.keys.Count / 2;

                //new node which into father node
                newValueInNode = new NewValueInNode<K>();
                newValueInNode.key = node.keys[mid];
                newValueInNode.value = node.values[mid];
                newValueInNode.node = new Node<K>();

                newValueInNode.node.isLeaf = node.isLeaf;

                //copy right node
                int movLen = node.keys.Count - mid - 1;
                newValueInNode.node.keys = node.keys.GetRange(mid + 1, movLen);
                newValueInNode.node.values = node.values.GetRange(mid + 1, movLen);
                node.keys.RemoveRange(mid, movLen + 1);
                node.values.RemoveRange(mid, movLen + 1);
                if (node.isLeaf == 0)
                {
                    newValueInNode.node.kids = node.kids.GetRange(mid + 1, movLen + 1);
                    node.kids.RemoveRange(mid + 1, movLen + 1);
                }
                return newValueInNode;
            }

            return null;
        }
    }
}
