using System.IO;
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

        public static string GetDirectoryName(string folderName) {
            //return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "viveksys");
            
            string directoryName = Path.Combine(Path.GetTempPath(), folderName);
            
            if (!Directory.Exists(directoryName)) {
                Directory.CreateDirectory(directoryName);
            }
            return directoryName;
        }

        public static string RenameFileName(string fileName, string oldValue, string newValue)
        {
            string destFileName = fileName.Replace(oldValue, newValue);
            File.Move(fileName, destFileName);
            return destFileName;
        }
    }


}
