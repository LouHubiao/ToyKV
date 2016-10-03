using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using NNanomsg;
using NNanomsg.Protocols;

namespace ToyGE
{
    class Program
    {
        //machines info
        static Machines<Int64> machines;

        //gap for every string or list
        static Int16 gap = 0;

        //group count of txs for pre set
        static int txsGroupCount = 50;

        //test keys
        static List<Int64> testKeys = new List<Int64>();

        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));
            initTx();
            Console.WriteLine("initTx()");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            string arg = args.Length != 0 ? args[0] : "error dic";
            LoadTx(arg);
            Console.WriteLine("LoadTx(args[0])");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            GetTest();
            Console.WriteLine("GetTest(args[0])");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            List<Int64> failedKeys = new List<Int64>();

            while (true)
            {
                Console.WriteLine("test: Please input type and value");
                string[] inputs = Console.ReadLine().Split(' ');
                if (inputs[0] == "search")
                {
                    Console.WriteLine("SearchNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    Int64[] keys = new Int64[] { Int64.Parse(inputs[1]) };

                    List<TX> txs = new List<TX>();
                    Task.Factory.StartNew(() =>
                    {
                        TxHelper.Get(keys, txs, failedKeys);
                    });

                    int timerCount = 0;
                    while (txs.Count + failedKeys.Count < keys.Length)
                    {
                        if (timerCount > 100)
                            break;
                        Thread.Sleep(timerCount++);
                    }

                    if (txs.Count != 0)
                        Console.WriteLine(txs[0].ToString());
                    else
                        Console.WriteLine("null!");
                    Console.WriteLine("SearchNode end..." + DateTime.Now.ToString("hh:mm:ss fff"));
                }
                else if (inputs[0] == "delete")
                {
                    Console.WriteLine("DeleteNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    TxHelper.Delete(Int64.Parse(inputs[1]));
                    Console.WriteLine("DeleteNode end..." + DateTime.Now.ToString("hh:mm:ss fff"));
                }
                else if (inputs[0] == "statistic")
                {
                    Console.WriteLine("statistic begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    if (inputs[1] == "count")
                    {
                        int count = TxHelper.Foreach(Statistic.Count_Statistic);
                        Console.WriteLine("Count_Statistic:" + count);
                    }
                    if (inputs[1] == "amount")
                    {
                        int count = TxHelper.Foreach(Statistic.Amount_Statistic);
                        Console.WriteLine("Amount_Statistic:" + count);
                    }
                    Console.WriteLine("statistic end..." + DateTime.Now.ToString("hh:mm:ss fff"));
                }
                Console.WriteLine();
            }

            //while (true)
            //{
            //    Console.WriteLine("test: Please input type and value");
            //    string[] inputs = Console.ReadLine().Split(' ');
            //    if (inputs[0] == "search")
            //    {
            //        Console.WriteLine("SearchNode begin..." + DateTime.Now);
            //        TX back = TxMain.SearchNode(Int64.Parse(inputs[1]));
            //        if (back != null)
            //            Console.WriteLine(back.ToString());
            //        else
            //            Console.WriteLine("null!");
            //        Console.WriteLine("SearchNode end..." + DateTime.Now);
            //    }
            //    else if (inputs[0] == "delete")
            //    {
            //        Console.WriteLine("DeleteNode begin..." + DateTime.Now);
            //        TxMain.DeleteNode(Int64.Parse(inputs[1]));
            //        Console.WriteLine("DeleteNode end..." + DateTime.Now);
            //    }
            //    else if (inputs[0] == "statistic")
            //    {
            //        Console.WriteLine("statistic begin..." + DateTime.Now);
            //        if (inputs[1] == "count")
            //        {
            //            int count = TxMain.Foreach(Statistic.Count_Statistic, 0, 0);
            //            Console.WriteLine("Count_Statistic:" + count);
            //        }
            //        if (inputs[1] == "amount")
            //        {
            //            Int64 amount = Int64.Parse(inputs[2]);
            //            int count = TxMain.Foreach(Statistic.Amount_Statistic, amount, 0);
            //            Console.WriteLine("Amount_Statistic:" + count);
            //        }
            //        Console.WriteLine("statistic end..." + DateTime.Now);
            //    }
            //    else if (inputs[0] == "update")
            //    {
            //        Console.WriteLine("Update begin..." + DateTime.Now);
            //        if (inputs[2] == "amount")
            //        {
            //            Int64 key = Int64.Parse(inputs[1]);
            //            Int64 newAmount = Int64.Parse(inputs[3]);
            //            TxMain.UpdateAmount(key, newAmount);
            //        }
            //        else if (inputs[2] == "hash")
            //        {
            //            Int64 key = Int64.Parse(inputs[1]);
            //            string newHash = inputs[3];
            //            TxMain.UpdateHash(key, newHash);
            //        }
            //        Console.WriteLine("Update end..." + DateTime.Now);
            //    }
            //    Console.WriteLine();
            //}
        }

        static void initTx()
        {
            int txPort = 7788;
            TxHelper.gap = 0;

            //<ip, memory space>
            Dictionary<UInt32, Int64> machineInventory = new Dictionary<UInt32, Int64>();
            //local: 10.172.154.30
            UInt32 IP1 = BitConverter.ToUInt32(IPAddress.Parse("10.172.154.30").GetAddressBytes(), 0);
            machineInventory.Add(IP1, (Int64)1 << 32);
            //remote: 10.86.170.172
            UInt32 IP2 = BitConverter.ToUInt32(IPAddress.Parse("10.172.96.46").GetAddressBytes(), 0);
            machineInventory.Add(IP2, (Int64)1 << 32);
            //graph21: 10.190.172.115
            UInt32 IP3 = BitConverter.ToUInt32(IPAddress.Parse("10.190.172.115").GetAddressBytes(), 0);
            machineInventory.Add(IP3, (Int64)1 << 32);

            //exclude localIP
            List<UInt32> localIPs = new List<UInt32>();
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    UInt32 localIP = BitConverter.ToUInt32(ip.GetAddressBytes(), 0);
                    if (!localIPs.Contains(localIP))
                        localIPs.Add(localIP);
                    if (localIP == 583532201)
                        localIPs.Add(IP3);
                }
            }
            foreach(UInt32 ip in localIPs)
            {
                string IPStr = new IPAddress(BitConverter.GetBytes(ip)).ToString();
                Console.WriteLine("localIP:" + IPStr);
            }

            //init the machines
            TxHelper.machines = new MachinesInt64(1 << 30, machineInventory, localIPs);

            //init the sockets
            TxHelper.repSocket.Bind("tcp://*:" + txPort);

            foreach (var item in machineInventory)
            {
                if (!localIPs.Contains(item.Key))
                {
                    RequestSocket reqSocket = new RequestSocket();
                    string IP = new IPAddress(BitConverter.GetBytes(item.Key)).ToString();
                    reqSocket.Connect("tcp://" + IP + ":" + txPort);
                    TxHelper.reqSockets.Add(item.Key, reqSocket);
                }
            }

            listenBegin();
        }

        //listen remote
        static void listenBegin()
        {
            Thread listener = new Thread(() =>
            {
                //begin listening
                TxHelper.Response();
            });
            listener.Start();
        }

        //load tx files
        static unsafe void LoadTx(string dic)
        {
            //load staitc floder
            //test: D:\\Bit\\TSLBit\\Generator\\bin\\x64\\Debug\\test
            //full: D:\\Bit\\TSLBit\\Generator\\bin\x64\\Debug\\fullBlocks
            //remote: D:\\v-hulou\\fullBlocks
            DirectoryInfo dirInfo = new DirectoryInfo(dic);
            int count = 0;

            ThreadPool.SetMaxThreads(8, 8);
            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine("workerThreads:{0}, completionPortThreads:{1}", workerThreads, completionPortThreads);

            if (dirInfo.Exists)
            {
                foreach (FileInfo file in dirInfo.GetFiles("*.txt"))
                {
                    //read json line by line
                    using (StreamReader reader = new StreamReader(file.FullName))
                    {
                        string line;
                        IntPtr preAddr = new IntPtr(0);
                        List<TX> txs = new List<TX>();
                        List<TX> outResults = new List<TX>(); ;
                        while (null != (line = reader.ReadLine()))
                        {
                            count++;

                            //string to object
                            TX jsonBack = TX.ConvertStringToJSONBack(line);

                            if (count % 10000 == 0)
                            {
                                testKeys.Add(jsonBack.CellID);
                            }

                            if (txs.Count < txsGroupCount)
                            {
                                //append
                                txs.Add(jsonBack);
                            }
                            else
                            {
                                //insert one node into memory
                                TX[] txsArr = txs.ToArray();
                                //ThreadPool.QueueUserWorkItem(
                                //    o => TxHelper.Set(txsArr, outResults)
                                //    );
                                TxHelper.Set(txsArr, outResults);
                                //clear txs
                                txs.Clear();
                                txs.Add(jsonBack);
                            }
                        }
                        if (txs.Count > 0)
                        {
                            //insert one node into memory
                            TX[] txsArr = txs.ToArray();
                            //ThreadPool.QueueUserWorkItem(
                            //        o => TxHelper.Set(txsArr, outResults)
                            //        );
                            TxHelper.Set(txsArr, outResults);
                            //clear txs
                            txs.Clear();
                        }

                        //failed part insert
                        if (outResults.Count > 0)
                        {

                        }
                    }
                }

            }
            Console.WriteLine("load count:{0}", count);
        }

        //test get cost
        static void GetTest()
        {
            Console.WriteLine("test count:{0}", testKeys.Count);

            Int64[] keysArr = testKeys.ToArray();
            List<TX> outTxs = new List<TX>();
            List<Int64> failedKeys = new List<Int64>();

            TxHelper.Get(keysArr, outTxs, failedKeys);
        }

    }
}
