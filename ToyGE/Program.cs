using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace ToyGE
{
    class Program
    {
        //machines info
        static Machines<Int64> machines;

        //gap for every string or list
        static Int16 gap = 0;

        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));
            initTx();
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));
            LoadTx(args[0]);
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            while (true)
            {
                Console.WriteLine("test: Please input type and value");
                string[] inputs = Console.ReadLine().Split(' ');
                if (inputs[0] == "search")
                {
                    Console.WriteLine("SearchNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    Int64[] keys = new Int64[] { Int64.Parse(inputs[1]) };
                    List<TX> txs = new List<TX>();
                    TxHelper.Get(keys, ref txs);
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
            TxHelper.txPort = 7788;
            TxHelper.gap = 0;

            //<ip, memory space>
            Dictionary<UInt32, int> machineInventory = new Dictionary<UInt32, int>();
            //local: 10.172.154.30
            UInt32 localIP = BitConverter.ToUInt32(IPAddress.Parse("10.172.154.30").GetAddressBytes(), 0);
            machineInventory.Add(localIP, 1 << 30);
            //remote: 10.86.170.172
            UInt32 remoteIP = BitConverter.ToUInt32(IPAddress.Parse("10.172.96.46").GetAddressBytes(), 0);
            machineInventory.Add(remoteIP, 1 << 30);
            TxHelper.machines = new MachinesInt64(1 << 30, machineInventory);

            listenBegin();
        }

        //load tx files
        static unsafe void LoadTx(string dic)
        {
            Console.WriteLine("LoadTxs begin...");

            //load staitc floder
            //test: D:\\Bit\\TSLBit\\Generator\\bin\\x64\\Debug\\test
            //full: D:\\Bit\\TSLBit\\Generator\\bin\x64\\Debug\\fullBlocks
            //remote: D:\\v-hulou\\fullBlocks
            DirectoryInfo dirInfo = new DirectoryInfo(dic);
            foreach (FileInfo file in dirInfo.GetFiles("*.txt"))
            {
                //read json line by line
                using (StreamReader reader = new StreamReader(file.FullName))
                {
                    string line;
                    IntPtr preAddr = new IntPtr(0);
                    int txsIndex = 0;
                    TX[] txs = new TX[50];
                    List<TX> outResults = new List<TX>();
                    while (null != (line = reader.ReadLine()))
                    {
                        //string to object
                        TX jsonBack = TX.ConvertStringToJSONBack(line);

                        if (txsIndex < txs.Length)
                        {
                            //append
                            txs[txsIndex++] = jsonBack;
                        }
                        else
                        {
                            //insert one node into memory
                            TxHelper.Set(txs, ref outResults);

                            //failed part insert
                            if (outResults.Count > 0)
                            {

                            }

                            //clear txs
                            txsIndex = 0;
                            txs[txsIndex++] = jsonBack;
                        }


                        //foreach (string _out in jsonBack.outs)
                        //{
                        //    IntPtr nodeAddr = new IntPtr();
                        //    if (UserMain.hashTree.BTSearch(UserMain.hashTree.root, _out, ref nodeAddr))
                        //    {
                        //        //insert list part

                        //    }
                        //    else
                        //    {
                        //        //insert new list
                        //        List<Int64> txs = new List<Int64>();
                        //        txs.Add(jsonBack.CellID);
                        //        UserMain.InsertUser_Cell_Index(_out, txs);
                        //    }
                        //}

                    }
                }
            }
            Console.WriteLine("LoadTxs end...");
        }

        static void listenBegin()
        {
            Thread listener = new Thread(() =>
            {
                //begin listening
                TxHelper.Response();
            });
            listener.Start();
        }

    }
}
