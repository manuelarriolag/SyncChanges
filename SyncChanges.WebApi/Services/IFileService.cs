using Microsoft.AspNetCore.Mvc;
using SyncChanges.WebApi.Model;

namespace SyncChanges.WebApi.Services
{
    public interface IFileService
    {
        public Task<string> PostFileAsync(IFormFile fileData, FileTypeEnum fileType);

        public Task<string[]> PostMultiFileAsync(List<FileUploadModel> fileData);

    }
}
