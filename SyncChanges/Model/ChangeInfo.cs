using System;
using System.Collections.Generic;

namespace SyncChanges.Model
{
    /// <summary>
    /// 
    /// </summary>
    [Serializable]
    public class ChangeInfo
    {
        public long Version { get; set; }
        public List<Change> Changes { get; private set; } = new List<Change>();
        public string FileName { get; set; }
    }
}