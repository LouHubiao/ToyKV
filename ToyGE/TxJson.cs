﻿using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Text;
using System.Collections;

/*	
    In Memory:

    Tx {
        sumLen      int
        CellID	    Int64
        hashLen     int
        hash	    string
        time	    Int64
        insLen      int
        ins		    calculated
        outsLen     int
        outs	    calculated
        amount	    Int64
    }

    In{
        addrLen     int
        addr        Int64
        tx_index    Int64
    }

    Out{
        addrLen     int
        addr        Int64
    }
*/

namespace ToyGE
{
    public class TxReq
    {
        [JsonProperty("reqType")]
        public string reqType;

        [JsonProperty("body")]
        public string body;
    }

    public class TX: IComparer
    {
        [JsonProperty("CellID")]
        public Int64 CellID;

        [JsonProperty("hash")]
        public String hash;

        [JsonProperty("time")]
        public Int64 time;

        [JsonProperty("ins")]
        public List<In> ins;

        [JsonProperty("outs")]
        public List<string> outs;

        [JsonProperty("amount")]
        public Int64 amount;

        //convert to jsonback from a string
        public static TX ConvertStringToJSONBack(string jsonStr)
        {
            TX tx = new TX();
            try
            {
                tx = JsonConvert.DeserializeObject<TX>(jsonStr);
            }
            catch (Exception e)
            {
                return null;
            }
            return tx;
        }

        public int GetLength()
        {
            int result = 0;
            result += 45;
            result += 3 + this.hash.Length;
            foreach (In _in in this.ins)
            {
                result += _in.addr.Length;
                result += 8;
            }
            foreach (string _out in this.outs)
            {
                result += _out.Length;
            }
            result += 8;

            return result;
        }

        public override string ToString()
        {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append("{\"CellID\":");
            strBuilder.Append(this.CellID);
            strBuilder.Append(",\"hash\":");
            strBuilder.Append("\"" + this.hash + "\"");
            strBuilder.Append(",\"time\":");
            strBuilder.Append(this.time);
            strBuilder.Append(",\"ins\":[");
            foreach (In _in in this.ins)
            {
                strBuilder.Append("{\"addr\":");
                strBuilder.Append("\"" + _in.addr + "\"");
                strBuilder.Append(",\"tx_index\":");
                strBuilder.Append(_in.tx_index);
                strBuilder.Append("},");
            }
            strBuilder.Append("],\"outs\":[");
            foreach (string _out in this.outs)
            {
                strBuilder.Append("\"" + _out + "\"");
                strBuilder.Append(",");
            }
            strBuilder.Append("],\"amount\":");
            strBuilder.Append(this.amount);
            strBuilder.Append("}");

            return strBuilder.ToString();
        }

        int Compare(object obj1, object obj2)
        {
            TX tx1 = obj1 as TX;
            TX tx2 = obj2 as TX;
            if (tx1.CellID > tx2.CellID)
                return 1;
            if (tx1.CellID < tx2.CellID)
                return -1;
            else
                return 0;
        }

    }

    public class In
    {
        public In(string _addr, Int64 _tx_index)
        {
            this.addr = _addr;
            this.tx_index = _tx_index;
        }

        [JsonProperty("addr")]
        public string addr;

        [JsonProperty("tx_index")]
        public Int64 tx_index;
    }
}
