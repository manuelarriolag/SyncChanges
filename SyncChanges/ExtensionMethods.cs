using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;


namespace SyncChanges
{

    public static class ExtensionMethods
    {
        public static T DeepCopy<T>(this T self)
        {
            var serialized = JsonConvert.SerializeObject(self);
            return JsonConvert.DeserializeObject<T>(serialized);
        }
    }


}
