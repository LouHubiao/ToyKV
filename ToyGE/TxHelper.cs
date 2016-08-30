using System;
using System.Collections.Generic;
using NNanomsg;
using NNanomsg.Protocols;
using System.Text;
using System.Net;
using Newtonsoft.Json;

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
        public static Machines<Int64> machines;
        //tx insert gap
        public static Int16 gap;

        #region search operation
        //convert memory Tx to object for random access
        public static bool Get(Int64[] keys, ref List<TX> txs)
        {
            //the out txs art not same order with keys 
            Array.Sort(keys);

            //get tx Addr
            IntPtr cellAddr;
            MachineIndex<Int64> machineIndex;
            UInt32 pendingIP = 0;
            List<Int64> pendingKeys = new List<Int64>();
            foreach (Int64 key in keys)
            {
                if (Machines<Int64>.Get(machines.machineIndexs, key, Compare.CompareInt64, out cellAddr, out machineIndex) == false)
                {
                    if (machineIndex.machineIP != 0)
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
                                        txs.Add(responseTx);
                                    }
                                }
                            }
                            //update pending info
                            pendingIP = machineIndex.machineIP;
                            pendingKeys.Clear();
                            pendingKeys.Add(key);
                        }
                    }
                    else
                    {
                        //get cell in local machine
                        TX tx = new TX();

                        // judge isDelete
                        byte status = MemByte.Get(ref cellAddr);
                        byte mask = 0x80;
                        if ((status & mask) != 0)
                        {
                            //deleted cell
                            continue;
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

                        txs.Add(tx);
                    }
                }
                else
                {
                    //error
                    return false;
                }
            }

            if (pendingKeys.Count != 0)
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
                        txs.Add(responseTx);
                    }
                }
            }

            return true;
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

        #endregion search operation


        #region insert operation
        //convert object to byte[] in memory
        public static bool Set(TX[] txs, ref List<int> setResults)
        {
            //sort txs, for insert multiply txs
            Array.Sort(txs);

            //get block info
            IntPtr cellAddr;
            MachineIndex<Int64> machineIndex;
            UInt32 pendingIP = 0;
            List<TX> pendingTxs = new List<TX>();
            foreach (TX tx in txs)
            {
                if (Machines<Int64>.Get(machines.machineIndexs, tx.CellID, Compare.CompareInt64, out cellAddr, out machineIndex) == false)
                {
                    if (machineIndex.machineIP != 0)
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
                                    int[] responses = JsonConvert.DeserializeObject<int[]>(responseStr);
                                    foreach (int response in responses)
                                    {
                                        setResults.Add(response);
                                    }
                                }
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
                        //judge if has enough space for just cell 37
                        Block<Int64> block = machineIndex.block;
                        IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, 37);
                        if (nextFreeInBlock.ToInt64() == 0)
                            return false;   //update false

                        B_Tree<Int64, IntPtr>.Insert(ref block.blockIndex.root, tx.CellID, nextFreeInBlock, Compare.CompareInt64);

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

                        setResults.Add(1);
                    }
                }
                else
                {
                    //if has cell, update it
                }
            }

            if (pendingTxs.Count != 0)
            {
                //send pending req
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
                    int[] responses = JsonConvert.DeserializeObject<int[]>(responseStr);
                    foreach (int response in responses)
                    {
                        setResults.Add(response);
                    }
                }
            }

            return true;
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
                        List<TX> txs = new List<TX>();
                        if (Get(keys, ref txs))
                        {
                            foreach (TX tx in txs)
                            {
                                responseStr.Append(tx.ToString() + ",");
                            }
                        }
                        else
                        {

                        }
                    }
                    else if (receiveOjb.reqType == "set")
                    {
                        //SET response
                        TX[] txs = JsonConvert.DeserializeObject<TX[]>(receiveOjb.body);
                        List<int> setResults = new List<int>();
                        if (Set(txs, ref setResults))
                        {
                            foreach (int setResult in setResults)
                            {
                                responseStr.Append(setResult.ToString() + ",");
                            }

                        }
                        else
                        {

                        }
                    }
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
            MachineIndex<Int64> machineIndex;
            if (Machines<Int64>.Get(machines.machineIndexs, key, Compare.CompareInt64, out cellAddr, out machineIndex) == false)
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
            foreach (MachineIndex<Int64> index in machines.machineIndexs.Values)
            {
                Block<Int64> block = index.block;
                Node<Int64, IntPtr> node = block.blockIndex.root;
                result += BTreeForeach(node, statistic);
            }
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
