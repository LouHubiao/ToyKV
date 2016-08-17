using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

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
            initMachines();
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));
            LoadTxs();
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss fff"));

            while (true)
            {
                Console.WriteLine("test: Please input type and value");
                string[] inputs = Console.ReadLine().Split(' ');
                if (inputs[0] == "search")
                {
                    Console.WriteLine("SearchNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    TX tx;
                    TxHelper.Get(Int64.Parse(inputs[1]), machines.machineIndex, out tx);
                    if (tx != null)
                        Console.WriteLine(tx.ToString());
                    else
                        Console.WriteLine("null!");
                    Console.WriteLine("SearchNode end..." + DateTime.Now.ToString("hh:mm:ss fff"));
                }
                else if (inputs[0] == "delete")
                {
                    Console.WriteLine("DeleteNode begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    TxHelper.Delete(Int64.Parse(inputs[1]), machines.machineIndex);
                    Console.WriteLine("DeleteNode end..." + DateTime.Now.ToString("hh:mm:ss fff"));
                }
                else if (inputs[0] == "statistic")
                {
                    Console.WriteLine("statistic begin..." + DateTime.Now.ToString("hh:mm:ss fff"));
                    if (inputs[1] == "count")
                    {
                        int count = TxHelper.Foreach(machines.machineIndex, Statistic.Count_Statistic);
                        Console.WriteLine("Count_Statistic:" + count);
                    }
                    if (inputs[1] == "amount")
                    {
                        int count = TxHelper.Foreach(machines.machineIndex, Statistic.Amount_Statistic);
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

        static void initMachines()
        {
            Dictionary<int, Int64> machineInfo = new Dictionary<int, Int64>();
            machineInfo.Add(1, (Int64)1 << 32);
            machines = new Machines<Int64>(1 << 30, machineInfo);
        }

        //load tx files
        static unsafe void LoadTxs()
        {
            Console.WriteLine("LoadTxs begin...");

            //load staitc floder
            //test: D:\\Bit\\TSLBit\\Generator\\bin\\x64\\Debug\\test
            //full: D:\\Bit\\TSLBit\\Generator\\bin\x64\\Debug\\fullBlocks
            //remote: D:\\v-hulou\\fullBlocks
            DirectoryInfo dirInfo = new DirectoryInfo(@"D:\\Bit\\TSLBit\\Generator\\bin\x64\\Debug\\test");
            foreach (FileInfo file in dirInfo.GetFiles("block90000.txt"))
            {
                //read json line by line
                using (StreamReader reader = new StreamReader(file.FullName))
                {
                    string line;
                    IntPtr preAddr = new IntPtr(0);
                    while (null != (line = reader.ReadLine()))
                    {
                        //string to object
                        TX jsonBack = TX.ConvertStringToJSONBack(line);

                        //insert one node into memory
                        TxHelper.Set(jsonBack, machines.machineIndex, gap);

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


    }
}
