using System;
using System.Collections.Generic;
using NNanomsg;
using NNanomsg.Protocols;
using System.Text;
using System.Net;
using Newtonsoft.Json;
using System.Threading;
using System.Threading.Tasks;

/*	
    In Memory:

    Tx {
        status      byte
        CellID      Int64
        hash        int32   // =>hash
        time        Int64
        ins         int32   // =>ins
        outs        int32   // =>outs
        amount      Int64
    }

    hash{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }

    ins{
        status      byte
        length      int16
        context     int32[] //=>in
        [curLnegth] int32
        [nextPart]  int32
    }

    in{
        status      byte
        addr        int32   // =>in_addr
        tx_index    Int64
    }

    in_addr{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }

    outs{
        status      byte
        length      int16
        context     int32[] //=>out
        [curLnegth] int32
        [nextPart]  int32

    }

    out{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }
*/

namespace ToyGE
{
    public class TxHelper
    {
        //must first init
        //tx port for listen
        public static int txPort = 0;
        //machines with tx index
        public static MachinesInt64 machines;
        //tx insert gap
        public static Int16 gap;

        #region search operation
        //convert memory Tx to object for random access
        public static void Get(Int64[] keys, List<TX> getTxs, List<Int64> failedKeys)
        {
            //the out txs art not same order with keys 
            Array.Sort(keys);

            MachineIndexInt64 machineIndex;
            UInt32 pendingIP = 0;
            List<Int64> pendingKeys = new List<Int64>();

            foreach (Int64 key in keys)
            {
                if (MachinesInt64.GetMachineIndex(machines.machineIndexs, key, out machineIndex) == false)
                {
                    //remote get
                    if (machineIndex.machineIP == pendingIP)
                    {
                        //add into pending req
                        pendingKeys.Add(key);
                    }
                    else
                    {
                        if (pendingKeys.Count != 0)
                        {
                            Int64[] pendingKeysArr = pendingKeys.ToArray();
                            Task.Factory.StartNew(() =>
                            {
                                GetRemote(pendingIP, pendingKeysArr, getTxs, failedKeys);
                            });
                        }
                        //update pending info
                        pendingIP = machineIndex.machineIP;
                        pendingKeys.Clear();
                        pendingKeys.Add(key);
                    }
                }
                else
                {
                    Task.Factory.StartNew(() =>
                    {
                        GetOneTx(machineIndex, key, getTxs, failedKeys);
                    });
                }
            }

            if (pendingKeys.Count != 0)
            {
                Int64[] pendingKeysArr = pendingKeys.ToArray();
                Task.Factory.StartNew(() =>
                {
                    GetRemote(pendingIP, pendingKeysArr, getTxs, failedKeys);
                });
            }
        }

        //get In struct
        public static In GetIn(ref IntPtr inAddr)
        {
            IntPtr offsetMemAddr = MemTool.GetOffsetedAddr(ref inAddr);

            byte status = MemByte.Get(ref offsetMemAddr);

            string addr = MemString.Get(ref offsetMemAddr);

            Int64 tx_index = MemInt64.Get(ref offsetMemAddr);

            return new In(addr, tx_index);
        }

        private static void GetOneTx(MachineIndexInt64 machineIndex, Int64 key, List<TX> getTxs, List<Int64> failedKeys)
        {
            //get tx Addr
            IntPtr cellAddr;

            if (MachinesInt64.GetCellAddr(machineIndex, key, out cellAddr))
            {
                //get cell in local machine
                TX tx = new TX();

                // judge isDelete
                byte status = MemByte.Get(ref cellAddr);
                byte mask = 0x80;
                if ((status & mask) != 0)
                {
                    //deleted cell
                    lock (failedKeys)
                    {
                        failedKeys.Add(key);
                    }
                    return;
                }

                //read cellID
                tx.CellID = MemInt64.Get(ref cellAddr);

                //read hash
                tx.hash = MemString.Get(ref cellAddr);

                //read time
                tx.time = MemInt64.Get(ref cellAddr);

                //read ins
                tx.ins = MemList.Get<In>(ref cellAddr, GetIn);

                //read outs
                tx.outs = MemList.Get<string>(ref cellAddr, MemString.Get);

                //time amount
                tx.amount = MemInt64.Get(ref cellAddr);

                lock (getTxs)
                {
                    getTxs.Add(tx);
                }
            }
        }

        private static void GetRemote(UInt32 pendingIP, Int64[] pendingKeys, List<TX> getTxs, List<Int64> failedKeys)
        {
            //send pending req and get tx array
            using (var req = new RequestSocket())
            {
                string ipAddress = new IPAddress(BitConverter.GetBytes(pendingIP)).ToString();
                req.Connect("tcp://" + ipAddress + ":" + txPort + "");
                //generate request str
                TxReq txReq = new TxReq();
                txReq.reqType = "get";
                StringBuilder body = new StringBuilder("[");
                foreach (Int64 pendingKey in pendingKeys)
                {
                    body.Append(pendingKey.ToString() + ',');
                }
                body.Remove(body.Length - 1, 1);
                body.Append(']');
                txReq.body = body.ToString();
                string reqStr = JsonConvert.SerializeObject(txReq);
                req.Send(Encoding.Default.GetBytes(reqStr.ToString()));
                string responseStr = Encoding.Default.GetString(req.Receive());
                TX[] responseTxs = JsonConvert.DeserializeObject<TX[]>(responseStr);
                foreach (TX responseTx in responseTxs)
                {
                    lock (getTxs)
                    {
                        getTxs.Add(responseTx);
                    }
                }
            }
        }

        #endregion search operation


        #region insert operation
        /// <summary>
        /// convert object to byte[] in memory
        /// </summary>
        /// <param name="txs"></param>
        /// <param name="failedTxs">failed insert txs</param>
        /// <returns></returns>
        public static void Set(TX[] txs, List<TX> failedTxs)
        {
            //sort txs, for insert multiply txs in nearby machine
            Array.Sort(txs);

            MachineIndexInt64 machineIndex;
            UInt32 pendingIP = 0;
            List<TX> pendingTxs = new List<TX>();
            foreach (TX tx in txs)
            {
                if (MachinesInt64.GetMachineIndex(machines.machineIndexs, tx.CellID, out machineIndex) == false)
                {
                    //remote get
                    if (machineIndex.machineIP == pendingIP)
                    {
                        //add into pending req
                        pendingTxs.Add(tx);
                    }
                    else
                    {
                        if (pendingTxs.Count != 0)
                        {
                            //send pending req
                            Task.Factory.StartNew(() =>
                            {
                                SetRemote(pendingIP, pendingTxs, failedTxs);
                            });
                        }
                        //update pending info
                        pendingIP = machineIndex.machineIP;
                        pendingTxs.Clear();
                        pendingTxs.Add(tx);
                    }
                }
                else
                {
                    //insert cell
                    Task.Factory.StartNew(() =>
                    {
                        SetOneTx(tx, machineIndex, failedTxs);
                    });
                }
            }

            if (pendingTxs.Count != 0)
            {
                //send pending req
                Task.Factory.StartNew(() =>
                {
                    SetRemote(pendingIP, pendingTxs, failedTxs);
                });
            }
        }

        private static void SetOneTx(TX tx, MachineIndexInt64 machineIndex, List<TX> failedTxs)
        {
            //debug
            if (tx.CellID == 148108)
            {

            }

            //judge if has enough space for just cell 37
            BlockInt64 block = machineIndex.block;
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, 37);
            if (nextFreeInBlock.ToInt64() == 0)
            {
                //find another free space
                lock (failedTxs)
                {
                    failedTxs.Add(tx);
                }
                return;
            }

            ARTInt64.Insert(block.blockIndex.tree, tx.CellID, nextFreeInBlock);

            //pointer for insert unsure length type, 37 is the length of tx
            IntPtr nextPartAddr = nextFreeInBlock + 37;

            //insert cellStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert CellID
            MemInt64.Set(ref nextFreeInBlock, tx.CellID);

            //insert hash(X)
            MemString.Set(ref nextFreeInBlock, tx.hash, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap);

            //insert time
            MemInt64.Set(ref nextFreeInBlock, tx.time);

            //insert ins(X)
            MemList.Set<In>(ref nextFreeInBlock, tx.ins, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, SetIn, null);

            //insert outs(X)
            MemList.Set<string>(ref nextFreeInBlock, tx.outs, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, MemString.Set, null);

            //insert amount
            MemInt64.Set(ref nextFreeInBlock, tx.amount);

        }

        /// <summary>
        /// remote set txs
        /// </summary>
        /// <param name="pendingIP">remote IP address</param>
        /// <param name="pendingTxs">insert txs</param>
        /// <param name="failedTxs">failed insert txs</param>
        /// <returns>false if error case</returns>
        private static void SetRemote(UInt32 pendingIP, List<TX> pendingTxs, List<TX> failedTxs)
        {
            using (var req = new RequestSocket())
            {
                string ipAddress = new IPAddress(BitConverter.GetBytes(pendingIP)).ToString();
                req.Connect("tcp://" + ipAddress + ":" + txPort + "");
                //generate request str
                TxReq txReq = new TxReq();
                txReq.reqType = "set";
                StringBuilder body = new StringBuilder("[");
                foreach (TX pendingTx in pendingTxs)
                {
                    body.Append(pendingTx.ToString() + ',');
                }
                body.Remove(body.Length - 1, 1);
                body.Append(']');
                txReq.body = body.ToString();
                string reqStr = JsonConvert.SerializeObject(txReq);
                req.Send(Encoding.Default.GetBytes(reqStr.ToString()));
                string responseStr = Encoding.Default.GetString(req.Receive());
                TX[] responses = JsonConvert.DeserializeObject<TX[]>(responseStr);
                if (responses.Length > 0)
                {
                    foreach (TX response in responses)
                    {
                        lock (failedTxs)
                        {
                            failedTxs.Add(response);
                        }
                    }
                }
            }
        }

        //insert In struct
        static bool SetIn(ref IntPtr memAddr, In input, IntPtr[] freeList, IntPtr headAddr, ref IntPtr tailAddr, Int32 blockLength, Int16 gap)
        {
            //judge if has enough space for just cell 13
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(freeList, headAddr, ref tailAddr, blockLength, 13);
            if (nextFreeInBlock.ToInt64() == 0)
                return false;   //update false

            //insert pointer
            MemInt32.Set(ref memAddr, (Int32)(nextFreeInBlock.ToInt64() - memAddr.ToInt64() - sizeof(Int32)));

            //struct length
            IntPtr nextNextPartAddr = nextFreeInBlock + 13;

            //insert inStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert in_addr
            MemString.Set(ref nextFreeInBlock, input.addr, freeList, headAddr, ref tailAddr, blockLength, gap);

            //insert tx_index
            MemInt64.Set(ref nextFreeInBlock, input.tx_index);

            return true;
        }
        #endregion insert operation

        #region remote response
        //response the remote request
        public static void Response()
        {
            using (var rep = new ReplySocket())
            {
                rep.Bind("tcp://*:" + txPort + "");
                var listener = new NanomsgListener();
                listener.AddSocket(rep);
                listener.ReceivedMessage += socketId =>
                {
                    string receiveJson = Encoding.Default.GetString(rep.Receive());
                    var receiveOjb = JsonConvert.DeserializeObject<TxReq>(receiveJson);
                    StringBuilder responseStr = new StringBuilder("[");

                    if (receiveOjb.reqType == "get")
                    {
                        //GET response
                        Int64[] keys = JsonConvert.DeserializeObject<Int64[]>(receiveOjb.body);
                        List<Int64> failedKeys = new List<Int64>();
                        List<TX> txs = new List<TX>();

                        //get inlocal
                        Task.Factory.StartNew(() =>
                        {
                            Get(keys, txs, failedKeys);
                        }).GetAwaiter().GetResult();

                        foreach (TX tx in txs)
                        {
                            responseStr.Append(tx.ToString() + ",");
                        }
                        if (failedKeys.Count > 0)
                        {
                            //get error
                        }
                    }
                    else if (receiveOjb.reqType == "set")
                    {
                        //SET response
                        TX[] txs = JsonConvert.DeserializeObject<TX[]>(receiveOjb.body);
                        List<TX> outResults = new List<TX>();

                        //set in local
                        Task.Factory.StartNew(() =>
                        {
                            Set(txs, outResults);
                        }).GetAwaiter().GetResult();
                        
                        if (outResults.Count > 0)
                        {
                            foreach (TX setResult in outResults)
                            {
                                responseStr.Append(setResult.ToString() + ",");
                            }
                        }
                        else
                        {

                        }
                    }
                    if (responseStr.Length > 1)
                        responseStr.Remove(responseStr.Length - 1, 1);
                    responseStr.Append("]");
                    rep.Send(Encoding.Default.GetBytes(responseStr.ToString()));
                };
                while (true)
                {
                    listener.Listen(null);
                }
            }
        }
        #endregion remote response

        #region delete operation
        //delete tx cell
        public static unsafe bool Delete(Int64 key)
        {
            //get tx Addr
            IntPtr cellAddr;
            MachineIndexInt64 machineIndex;
            if (MachinesInt64.GetMachineIndex(machines.machineIndexs, key, out machineIndex) == false)
            {
                return false;
            }
            if (MachinesInt64.GetCellAddr(machineIndex, key, out cellAddr) == false)
            {
                return false;
            }

            //update status IsDelete=1
            IntPtr memAddrCopy = cellAddr;
            byte* status = (byte*)(memAddrCopy.ToPointer());
            byte mask = 0x80;
            *status = (byte)(*status | mask);

            //jump status and CellID
            memAddrCopy = memAddrCopy + 1 + 8;

            //delete hash
            MemString.Delete(ref memAddrCopy, machineIndex.block.freeList);

            //jump time
            memAddrCopy = memAddrCopy + 8;

            //delete ins
            MemList.Delete<In>(ref memAddrCopy, machineIndex.block.freeList, DeleteIn);

            ////update cell link list
            //int length = 44;
            //if (length >= 64)
            //{
            //    IntPtr nextAddr = new IntPtr(memAddr.ToInt64() + *(Int32*)(memAddr + 1));
            //    Int32 preOffset = *(Int32*)(memAddr + 5);
            //    IntPtr preAddr = preOffset == 0 ? new IntPtr(0) : new IntPtr(memAddr.ToInt64() - preOffset);
            //    MemCell.UpdateNextNode_PreNode(nextAddr, preAddr);
            //}

            return true;
        }

        public static void DeleteIn(ref IntPtr memAddr, IntPtr[] freeAddrs)
        {
            IntPtr offsetMemAddr = MemTool.GetOffsetedAddr(ref memAddr);

            //jump status
            offsetMemAddr = offsetMemAddr + 1;

            //delete in_addr
            MemString.Delete(ref offsetMemAddr, freeAddrs);
        }
        #endregion delete operation

        #region foreach
        //foreach the index fo statistic
        public static int Foreach(Delegate<Int64>.Statistic statistic)
        {
            int result = 0;
            foreach (MachineIndexInt64 index in machines.machineIndexs.Values)
            {
                BlockInt64 block = index.block;
                ARTInt64Node node = block.blockIndex.tree.root;
                result += ARTTreeForeach(node, statistic);
            }
            return result;
        }

        static int ARTTreeForeach(ARTInt64Node node, Delegate<Int64>.Statistic statistic)
        {
            if (node == null)
            {
                return 0;
            }

            int result = 0;
            if (node.value.ToInt32() != 0)
            {
                if (statistic(node.value) == true)
                {
                    result++;
                }
            }
            if (node.leftChild != null)
                result += ARTTreeForeach(node.leftChild, statistic);
            if (node.rightChild != null)
                result += ARTTreeForeach(node.rightChild, statistic);
            return result;
        }

        //walking btree
        static int BTreeForeach(Node<Int64, IntPtr> node, Delegate<Int64>.Statistic statistic)
        {
            if (node == null || node.keys.Count == 0)
            {
                return 0;
            }

            int result = 0;
            for (int i = 0; i < node.keys.Count; i++)
            {
                if (statistic(node.values[i]) == true)
                {
                    result++;
                }

                if (node.kids.Count > i)
                    result += BTreeForeach(node.kids[i], statistic);
            }
            if (node.kids.Count > node.keys.Count)
                result += BTreeForeach(node.kids[node.keys.Count], statistic);
            return result;
        }
        #endregion foreach

        #region update operation
        //update hash
        //public static unsafe void UpdateHash(IntPtr memAddr, string newHash, IntPtr[] freeAdds)
        //{
        //    //pointer for hash
        //    memAddr += 17;
        //    MemString.Update(memAddr, newHash, freeAdds);
        //}

        ////update amount
        //public static unsafe void UpdateAmount(IntPtr memAddr, Int64 newAmount)
        //{
        //    //pointer for amount
        //    memAddr += 37;
        //    MemHelper.InsertValue(ref memAddr, newAmount);
        //}
        #endregion update operation
    }
}
