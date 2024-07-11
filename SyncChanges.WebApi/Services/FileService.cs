using SyncChanges.WebApi.Model;

namespace SyncChanges.WebApi.Services
{
    public class FileService : IFileService {
        
        private readonly IStorage _storage;

        public FileService(IStorage storage) {
            _storage = storage;
        }

        public async Task<string> PostFileAsync(IFormFile fileData, FileTypeEnum fileType)
        {
            string filePath = await _storage.Save(fileData);
            return filePath;
        }

        public async Task<string[]> PostMultiFileAsync(List<FileUploadModel> fileData)
        {
            List<string> files = new List<string>();
            foreach (FileUploadModel file in fileData)
            {
                string filePath = await _storage.Save(file.FileDetails);
                files.Add(filePath);
            }
            return files.ToArray();
        }

    }
}
