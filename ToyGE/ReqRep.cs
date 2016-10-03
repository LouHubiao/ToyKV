using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace ToyGE
{
    public class Request
    {
        [JsonProperty("reqType")]
        public string reqType;

        [JsonProperty("body")]
        public string body;
    }

    public class Response
    {
        [JsonProperty("results")]
        public string results;

        [JsonProperty("failedKeys")]
        public string failedKeys;
    }
}
