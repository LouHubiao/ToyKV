using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;

namespace ToyGE
{
    public interface Index<K>
    {
        /// <summary>
        /// Searches value in index by key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="result">The result.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        bool Search(K key, out IntPtr result);

        /// <summary>
        /// Inserts key and value into index.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        void Insert(K key, IntPtr value);
    }
}
