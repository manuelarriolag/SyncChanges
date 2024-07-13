using SyncChanges.Model;

namespace SyncChanges.WebApi.Model
{
    public class FileUploadModel
    {
        public IFormFile FileDetails { get; set; }
        public FileTypeEnum FileType { get; set; }
    }
}
