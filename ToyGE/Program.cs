using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using Newtonsoft.Json;
using NNanomsg;
using NNanomsg.Protocols;

namespace ToyGE
{
    class Program
    {
        //machines info
        static Machines<Int64> machines;

        //group count of txs for pre set
        static int txsGroupCount = 50;

        //test keys
        static List<Int64> testKeys = new List<Int64>();

        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));
            init();
            Console.WriteLine("initTx()");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            string arg = args.Length != 0 ? args[0] : "error dic";
            load(arg);
            Console.WriteLine("LoadTx()");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            test();
            Console.WriteLine("GetTest()");
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            while (true)
            {
                Console.WriteLine("test: Please input type and value");
                string[] inputs = Console.ReadLine().Split(' ');
                if (inputs[0] == "search")
                {
                    Console.WriteLine("SearchNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    Int64[] keys = new Int64[] { Int64.Parse(inputs[1]) };

                    List<TX> results = new List<TX>();
                    List<Int64> failedKeys = new List<Int64>();
                    TxHelper.Get(keys, new TxHelper.Filter[] { (TxHelper.Filter)Enum.Parse(typeof(TxHelper.Filter), "Hash", true) }, results, failedKeys);

                    if (results.Count != 0)
                        Console.WriteLine(JsonConvert.SerializeObject(results[0]));
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

        static void init()
        {
            //<ip, memory space>
            Dictionary<UInt32, Int64> machineInventory = new Dictionary<UInt32, Int64>();
            //local: 10.172.154.30
            UInt32 IP1 = BitConverter.ToUInt32(IPAddress.Parse("192.168.0.136").GetAddressBytes(), 0);
            machineInventory.Add(IP1, (Int64)1 << 32);
            ////local: 10.172.154.30
            //UInt32 IP1 = BitConverter.ToUInt32(IPAddress.Parse("10.172.154.30").GetAddressBytes(), 0);
            //machineInventory.Add(IP1, (Int64)1 << 32);
            ////remote: 10.86.170.172
            //UInt32 IP2 = BitConverter.ToUInt32(IPAddress.Parse("10.172.96.46").GetAddressBytes(), 0);
            //machineInventory.Add(IP2, (Int64)1 << 32);
            ////graph21: 10.190.172.115
            //UInt32 IP3 = BitConverter.ToUInt32(IPAddress.Parse("10.190.172.115").GetAddressBytes(), 0);
            //machineInventory.Add(IP3, (Int64)1 << 32);

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
                    //IP3 special
                    //if (localIP == 583532201)
                    //    localIPs.Add(IP3);
                }
            }
            //console local ip address
            //foreach (UInt32 ip in localIPs)
            //{
            //    string IPStr = new IPAddress(BitConverter.GetBytes(ip)).ToString();
            //    Console.WriteLine("localIP:" + IPStr);
            //}

            Int16 gap = 0;
            int port = 7788;

            TxHelper.init(machineInventory, localIPs, gap, port);
        }

        //load tx files
        static unsafe void load(string dic)
        {
            //load staitc floder
            DirectoryInfo dirInfo = new DirectoryInfo(dic);
            int count = 0;
            List<TX> values = new List<TX>();
            List<TX> failedValues = new List<TX>();

            //config max threads count
            //ThreadPool.SetMaxThreads(8, 8);
            //int workerThreads;
            //int completionPortThreads;
            //ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            //Console.WriteLine("workerThreads:{0}, completionPortThreads:{1}", workerThreads, completionPortThreads);

            if (dirInfo.Exists)
            {
                foreach (FileInfo file in dirInfo.GetFiles("*.txt"))
                {
                    //read json line by line
                    using (StreamReader reader = new StreamReader(file.FullName))
                    {
                        string line;
                        while (null != (line = reader.ReadLine()))
                        {
                            //string to object
                            TX jsonBack = JsonConvert.DeserializeObject<TX>(line);

                            //set
                            values.Add(jsonBack);
                            if (values.Count == txsGroupCount)
                            {
                                //insert one node into memory
                                //ThreadPool.QueueUserWorkItem(
                                //    o => TxHelper.Set(txsArr, outResults)
                                //    );
                                TxHelper.Set(values.ToArray(), null, failedValues);
                                //clear txs
                                values.Clear();
                            }

                            count++;
                            //get test ids
                            if (count % 10000 == 0)
                            {
                                testKeys.Add(jsonBack.NodeID);
                            }
                        }
                        //remain values
                        if (values.Count > 0)
                        {
                            TxHelper.Set(values.ToArray(), null, failedValues);
                            values.Clear();
                        }
                    }
                }

            }

            //failed part insert
            if (failedValues.Count > 0)
            {
                //hlou: set again in another thread
            }

            Console.WriteLine("load count:{0}", count);
        }

        //test get cost
        static void test()
        {
            Console.WriteLine("test count:{0}", testKeys.Count);

            Int64[] keysArr = testKeys.ToArray();
            List<TX> outTxs = new List<TX>();
            List<Int64> failedKeys = new List<Int64>();

            TxHelper.Get(keysArr, null, outTxs, failedKeys);
        }
    }
}
