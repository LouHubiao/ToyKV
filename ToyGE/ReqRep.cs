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
        [JsonProperty("type")]
        public string type;

        [JsonProperty("body")]
        public string body;
    }

    public class GetRequest
    {
        [JsonProperty("keys")]
        public string keys;

        [JsonProperty("header")]
        public string header;

        [JsonProperty("conditionHeader")]
        public string conditionHeader;

        [JsonProperty("conditions")]
        public string conditions;
    }

    public class SetRequest
    {
        [JsonProperty("header")]
        public string header;

        [JsonProperty("values")]
        public string values;
    }

    public class Response
    {
        [JsonProperty("results")]
        public string results;

        [JsonProperty("errorResults")]
        public string errorResults;
    }
}
