using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using Newtonsoft.Json;
using NNanomsg;
using NNanomsg.Protocols;
using System.Collections;
using System.Net;
using System.Threading;

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
    public class TX : IComparable<TX>
    {
        [JsonProperty("CellID")]
        public Int64 NodeID;

        [JsonProperty("hash")]
        public String Hash;

        [JsonProperty("time")]
        public Int64 Time;

        [JsonProperty("ins")]
        public List<In> In;

        [JsonProperty("outs")]
        public List<string> Out;

        [JsonProperty("amount")]
        public Int64 Amount;

        public int CompareTo(TX other)
        {
            return NodeID.CompareTo(other.NodeID);
        }
    }

    public class TxHelper
    {
        //machines with tx index，must first init
        public static MachinesInt64 machines;
        //txs gap in memory
        public static Int16 gap;
        //response socket by nanomsg
        public static ReplySocket repSocket = new ReplySocket();
        //request sockets with remote machines by nanomsg
        public static Dictionary<UInt32, RequestSocket> reqSockets = new Dictionary<UInt32, RequestSocket>();

        public enum Header
        {
            Hash = 1,
            Time = 2,
            In = 3,
            Out = 4,
            Amount = 5
        }

        #region init

        public static void init(Dictionary<UInt32, Int64> machineInventory, List<UInt32> localIPs, Int16 pGap, int port)
        {
            machines = new MachinesInt64(1 << 30, machineInventory, localIPs);
            //gap for string or list
            gap = pGap;
            repSocket.Bind("tcp://*:" + port);
            foreach (var item in machineInventory)
            {
                if (!localIPs.Contains(item.Key))
                {
                    RequestSocket reqSocket = new RequestSocket();
                    string IP = new IPAddress(BitConverter.GetBytes(item.Key)).ToString();
                    reqSocket.Connect("tcp://" + IP + ":" + port);
                    reqSockets.Add(item.Key, reqSocket);
                }
            }

            listenBegin();
        }
        #endregion

        #region search operation
        /// <summary>
        /// search in index and convert bytes into object
        /// </summary>
        /// <param name="keys">search keys</param>
        /// <param name="results">result objects</param>
        /// <param name="errorKeys">failed keys</param>
        public static void Get(Int64[] keys, Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results, List<Int64> errorKeys)
        {
            //pay attention: the out txs art not same order with keys 

            if (keys != null && keys.Length > 0)
            {
                //which machine has this key
                MachineIndexInt64 machineIndex;
                //sort keys for remote group get
                Array.Sort(keys);
                //temp ip for remote group get
                UInt32 pendingIP = 0;
                //part keys for remote group get
                List<Int64> pendingKeys = new List<Int64>();

                foreach (Int64 key in keys)
                {
                    //get index
                    if (MachinesInt64.GetMachineIndex(machines.machineIndexs, key, out machineIndex) == false)
                    {
                        //remote get
                        if (machineIndex.machineIP == pendingIP)
                        {
                            //add into pendingKeys if has same remote ip
                            pendingKeys.Add(key);
                        }
                        else
                        {
                            if (pendingKeys.Count != 0)
                            {
                                //remote get, hlou: multiple threads for different machines
                                GetRemote(pendingIP, pendingKeys.ToArray(), header, conditionHeader, conditions, results, errorKeys);
                            }
                            //update pending info
                            pendingIP = machineIndex.machineIP;
                            pendingKeys.Clear();
                            pendingKeys.Add(key);
                        }
                    }
                    else
                    {
                        //tx begin address
                        IntPtr cellAddr;

                        //get tx begin address
                        if (MachinesInt64.GetCellAddr(machineIndex, key, out cellAddr) == false)
                        {
                            //not found, add into errorKeys
                            errorKeys.Add(key);
                        }
                        else
                        {
                            //local get, hlou: multiple threads for speed up
                            if (GetOneCell(cellAddr, header, conditionHeader, conditions, results) == false)
                            {
                                //not found or not meet conditon
                                errorKeys.Add(key);
                            }
                        }
                    }
                }
                //remain keys
                if (pendingKeys.Count != 0)
                {
                    GetRemote(pendingIP, pendingKeys.ToArray(), header, conditionHeader, conditions, results, errorKeys);
                }
            }
            //has("property","value") function
            else
            {
                HasLocal(header, conditionHeader, conditions, results);
                HasRemote(header, conditionHeader, conditions, results);
            }
        }

        static void HasLocal(Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results)
        {
            foreach (MachineIndexInt64 index in machines.machineIndexs.Values)
            {
                if (index.machineIP == 0)
                {
                    ARTInt64Node node = index.block.index.tree.root;
                    HasLocalTraversal(node, header, conditionHeader, conditions, results);
                }
            }
        }

        static void HasRemote(Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results)
        {
            foreach (MachineIndexInt64 index in machines.machineIndexs.Values)
            {
                if (index.machineIP != 0)
                {
                    GetRemote(index.machineIP, null, header, conditionHeader, conditions, results, null);
                }
            }
        }

        static void HasLocalTraversal(ARTInt64Node node, Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results)
        {
            if (node == null)
            {
                return;
            }

            if (node.value.ToInt64() != 0)
            {
                GetOneCell(node.value, header, conditionHeader, conditions, results);
            }
            HasLocalTraversal(node.leftChild, header, conditionHeader, conditions, results);
            HasLocalTraversal(node.rightChild, header, conditionHeader, conditions, results);
        }

        /// <summary>
        /// get one objct of cell
        /// </summary>
        /// <param name="machineIndex">machine index tree</param>
        /// <param name="key">input key</param>
        /// <param name="results">return results(has value, must locked before update)</param>
        /// <param name="errorKeys">failed keys</param>
        private static bool GetOneCell(IntPtr cellAddr, Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results)
        {
            //pay attention: results and errorKeys has data before, for multiple threads speed up

            // judge isDelete
            byte status = MemByte.Get(ref cellAddr);
            byte mask = 0x80;
            if ((status & mask) != 0)
            {
                //deleted cell
                return false;
            }

            TX tx = new TX();

            //read CellID
            tx.NodeID = MemInt64.Get(ref cellAddr);

            //or results for multiple condtions
            bool[] orFails = new bool[(conditions == null) ? 0 : conditions.Length];

            //read Hash
            if (header == null || ((IList)header).Contains(Header.Hash))
            {
                tx.Hash = MemString.Get(ref cellAddr);
                if (conditionHeader != null && ((IList)conditionHeader).Contains(Header.Hash))
                {
                    for (int i = 0; i < conditions.Length; i++)
                    {
                        if (orFails[i] == false && conditions[i].Hash != null && tx.Hash != conditions[i].Hash)
                            orFails[i] = true;
                    }
                }
            }
            else
                MemString.Jump(ref cellAddr);

            //read Time
            if (header == null || ((IList)header).Contains(Header.Time))
            {
                tx.Time = MemInt64.Get(ref cellAddr);
                if (conditionHeader != null && ((IList)conditionHeader).Contains(Header.Time))
                {
                    for (int i = 0; i < conditions.Length; i++)
                    {
                        if (orFails[i] == false && conditions[i].Time != default(long) && tx.Time != conditions[i].Time)
                            orFails[i] = true;
                    }
                }
            }
            else
                MemInt64.Jump(ref cellAddr);

            //read ins
            if (header == null || ((IList)header).Contains(Header.In))
            {
                tx.In = MemList.Get<In>(ref cellAddr, InHelper.Get);
                if (conditionHeader != null && ((IList)conditionHeader).Contains(Header.In))
                {
                    for (int i = 0; i < conditions.Length; i++)
                    {
                        if (orFails[i] == false && conditions[i].In != null && tx.In.SequenceEqual(conditions[i].In) == false)
                            orFails[i] = true;
                    }
                }
            }
            else
                MemList.Jump(ref cellAddr);

            //read Out
            if (header == null || ((IList)header).Contains(Header.Out))
            {
                tx.Out = MemList.Get<string>(ref cellAddr, MemString.Get);
                if (conditionHeader != null && ((IList)conditionHeader).Contains(Header.Out))
                {
                    for (int i = 0; i < conditions.Length; i++)
                    {
                        if (orFails[i] == false && conditions[i].Out != null && tx.Out.SequenceEqual(conditions[i].Out) == false)
                            orFails[i] = true;
                    }
                }
            }
            else
                MemString.Jump(ref cellAddr);

            //read Amount
            if (header == null || ((IList)header).Contains(Header.Amount))
            {
                tx.Amount = MemInt64.Get(ref cellAddr);
                if (conditionHeader != null && ((IList)conditionHeader).Contains(Header.Amount))
                {
                    for (int i = 0; i < conditions.Length; i++)
                    {
                        if (orFails[i] == false && conditions[i].Amount != default(Int64) && tx.Amount != conditions[i].Amount)
                            orFails[i] = true;
                    }
                }
            }
            else
                MemInt64.Jump(ref cellAddr);

            if (orFails.Length == 0)
            {
                //lock results for multiple threads
                lock (results)
                {
                    results.Add(tx);
                }
                return true;
            }

            foreach (bool orFail in orFails)
            {
                //meet or condition
                if (orFail == false)
                {
                    //lock results for multiple threads
                    lock (results)
                    {
                        results.Add(tx);
                    }
                    return true;
                }
            }

            //not meet condition
            return false;
        }

        /// <summary>
        /// get results from remote machines
        /// </summary>
        /// <param name="remoteIP">remote machine ip to get</param>
        /// <param name="keys">keys to get</param>
        /// <param name="header">header filter</param>
        /// <param name="results">return results(has value, must locked before update)</param>
        /// <param name="errorKeys">failed keys</param>
        private static void GetRemote(UInt32 remoteIP, Int64[] keys, Header[] header, Header[] conditionHeader, TX[] conditions, List<TX> results, List<Int64> errorKeys)
        {
            //request socket inited before
            RequestSocket req = reqSockets[remoteIP];

            //generate request object, format:{reqType: get, body:[key1, key2, ...]}
            Request request = new Request();
            request.type = "get";
            GetRequest getRequest = new GetRequest();
            getRequest.keys = JsonConvert.SerializeObject(keys);
            getRequest.header = JsonConvert.SerializeObject(header);
            getRequest.conditionHeader = JsonConvert.SerializeObject(conditionHeader);
            getRequest.conditions = JsonConvert.SerializeObject(conditions);
            request.body = JsonConvert.SerializeObject(getRequest);

            //send
            req.Send(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request)));

            //receieve
            Response rep = JsonConvert.DeserializeObject<Response>(Encoding.UTF8.GetString(req.Receive()));

            //deserialize response
            TX[] appendResults = JsonConvert.DeserializeObject<TX[]>(rep.results);
            Int64[] appendFailedKeys = JsonConvert.DeserializeObject<Int64[]>(rep.errorResults);

            //add results
            foreach (TX append in appendResults)
            {
                //lock for multiple threads
                lock (results)
                {
                    results.Add(append);
                }
            }

            //add failedkeys
            foreach (Int64 append in appendFailedKeys)
            {
                lock (errorKeys)
                {
                    errorKeys.Add(append);
                }
            }
        }
        #endregion search operation


        #region insert operation
        /// <summary>
        /// convert object to byte[] in memory
        /// </summary>
        /// <param name="values">values to set</param>
        /// <param name="failedValues">failed insert values</param>
        public static void Set(TX[] values, Header[] filters, List<TX> failedValues)
        {
            //sort keys for remote group set
            Array.Sort(values);

            MachineIndexInt64 machineIndex;
            UInt32 pendingIP = 0;
            List<TX> pendingTxs = new List<TX>();
            foreach (TX value in values)
            {
                //get index
                if (MachinesInt64.GetMachineIndex(machines.machineIndexs, value.NodeID, out machineIndex) == false)
                {
                    //remote set
                    if (machineIndex.machineIP == pendingIP)
                    {
                        //add into pendingKeys if has same remote ip
                        pendingTxs.Add(value);
                    }
                    else
                    {
                        if (pendingTxs.Count != 0)
                        {
                            //remote set, hlou: multiple threads for different machines
                            SetRemote(pendingIP, pendingTxs, filters, failedValues);
                        }
                        //update pending info
                        pendingIP = machineIndex.machineIP;
                        pendingTxs.Clear();
                        pendingTxs.Add(value);
                    }
                }
                else
                {
                    //local set, hlou: multiple threads for speed up
                    SetOneCell(machineIndex, value, filters, failedValues);
                }
            }
            //remain keys
            if (pendingTxs.Count != 0)
            {
                //send pending req
                SetRemote(pendingIP, pendingTxs, filters, failedValues);
            }
        }

        /// <summary>
        /// set one objct of cell
        /// </summary>
        /// <param name="machineIndex">machine index tree</param>
        /// <param name="value">value to set</param>
        /// <param name="failedValues"></param>
        private static void SetOneCell(MachineIndexInt64 machineIndex, TX value, Header[] filters, List<TX> failedValues)
        {
            //judge if has enough space for cell (space=37, get before compile)
            BlockInt64 block = machineIndex.block;
            IntPtr nextFreeInBlock = MemFreeList.GetFreeInBlock<byte>(block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, 37);
            if (nextFreeInBlock.ToInt64() == 0)
            {
                lock (failedValues)
                {
                    failedValues.Add(value);
                }
                return;
            }

            //insert into index
            ARTInt64.Insert(block.index.tree, value.NodeID, nextFreeInBlock);

            //insert cellStatus
            MemByte.Set(ref nextFreeInBlock, (byte)0);

            //insert CellID
            MemInt64.Set(ref nextFreeInBlock, value.NodeID);

            //insert hash(X)
            if (filters == null || ((IList)filters).Contains(Header.Hash))
            {
                if (MemString.Set(ref nextFreeInBlock, value.Hash, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap) == false)
                {
                    lock (failedValues)
                    {
                        failedValues.Add(value);
                    }
                    return;
                }
            }
            else
                MemString.Jump(ref nextFreeInBlock);

            //insert time
            if (filters == null || ((IList)filters).Contains(Header.Time))
                MemInt64.Set(ref nextFreeInBlock, value.Time);
            else
                MemInt64.Jump(ref nextFreeInBlock);

            //insert ins(X)
            if (filters == null || ((IList)filters).Contains(Header.In))
            {
                if (MemList.Set<In>(ref nextFreeInBlock, value.In, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, InHelper.Set, null) == false)
                {
                    lock (failedValues)
                    {
                        failedValues.Add(value);
                    }
                    return;
                }
            }
            else
                MemList.Jump(ref nextFreeInBlock);

            //insert outs(X)
            if (filters == null || ((IList)filters).Contains(Header.Out))
            {
                if (MemList.Set<string>(ref nextFreeInBlock, value.Out, block.freeList, block.headAddr, ref block.tailAddr, block.blockLength, gap, MemString.Set, null) == false)
                {
                    lock (failedValues)
                    {
                        failedValues.Add(value);
                    }
                    return;
                }
            }
            else
                MemList.Jump(ref nextFreeInBlock);

            //insert amount
            if (filters == null || ((IList)filters).Contains(Header.Amount))
                MemInt64.Set(ref nextFreeInBlock, value.Amount);
            else
                MemInt64.Jump(ref nextFreeInBlock);

        }

        /// <summary>
        /// remote set txs
        /// </summary>
        /// <param name="remoteIP">remote machine ip to set</param>
        /// <param name="values">values to set</param>
        /// <param name="failedValues">values failed insert</param>
        private static void SetRemote(UInt32 remoteIP, List<TX> values, Header[] header, List<TX> failedValues)
        {
            //request socket inited before
            RequestSocket req = reqSockets[remoteIP];

            //generate request object, format:{reqType: get, body:[jsonObj1, jsonObj2, ...]}
            Request request = new Request();
            request.type = "set";
            SetRequest setRequest = new SetRequest();
            setRequest.header = JsonConvert.SerializeObject(header);
            setRequest.values = JsonConvert.SerializeObject(values);
            request.body = JsonConvert.SerializeObject(setRequest);

            //send
            req.Send(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request)));

            //receieve
            Response rep = JsonConvert.DeserializeObject<Response>(Encoding.UTF8.GetString(req.Receive()));

            //add failed results
            TX[] responses = JsonConvert.DeserializeObject<TX[]>(rep.errorResults);
            if (responses.Length > 0)
            {
                foreach (TX response in responses)
                {
                    lock (failedValues)
                    {
                        failedValues.Add(response);
                    }
                }
            }
        }
        #endregion insert operation

        #region remote response
        /// <summary>
        /// listen remote request
        /// </summary>
        static void listenBegin()
        {
            Thread listener = new Thread(() =>
            {
                //begin listening
                Response();
            });
            listener.Start();
        }

        /// <summary>
        /// response the remote request, in background thread
        /// </summary>
        public static void Response()
        {
            while (true)
            {
                //receieve
                string remoteReqStr = Encoding.UTF8.GetString(repSocket.Receive());
                Request remoteReq = JsonConvert.DeserializeObject<Request>(remoteReqStr);
                Response rep = new Response();

                //GET request
                if (remoteReq.type == "get")
                {
                    GetRequest getRequest = JsonConvert.DeserializeObject<GetRequest>(remoteReq.body);
                    Int64[] keys = JsonConvert.DeserializeObject<Int64[]>(getRequest.keys);
                    Header[] header = JsonConvert.DeserializeObject<Header[]>(getRequest.header);
                    Header[] conditionHeader = JsonConvert.DeserializeObject<Header[]>(getRequest.conditionHeader);
                    TX[] conditions = JsonConvert.DeserializeObject<TX[]>(getRequest.conditions);

                    List<TX> results = new List<TX>();
                    List<Int64> errorResults = new List<Int64>();

                    if (keys.Length > 0)
                    {
                        //get in local
                        Get(keys, header, conditionHeader, conditions, results, errorResults);
                    }
                    else
                    {
                        //has function
                        HasLocal(header, conditionHeader, conditions, results);
                    }

                    //generate response
                    rep.results = JsonConvert.SerializeObject(results);
                    rep.errorResults = JsonConvert.SerializeObject(errorResults);
                }
                //SET request
                else if (remoteReq.type == "set")
                {
                    SetRequest setRequest = JsonConvert.DeserializeObject<SetRequest>(remoteReq.body);
                    Header[] header = JsonConvert.DeserializeObject<Header[]>(setRequest.header);
                    TX[] values = JsonConvert.DeserializeObject<TX[]>(setRequest.values);

                    List<TX> errorResults = new List<TX>();

                    //set in local
                    Set(values, header, errorResults);

                    //generate response
                    rep.results = null;
                    rep.errorResults = JsonConvert.SerializeObject(errorResults);
                }
                repSocket.Send(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(rep)));
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
                ARTInt64Node node = block.index.tree.root;
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
