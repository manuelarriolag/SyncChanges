namespace SyncChanges.WebApi.Services;

public class FileSystemStorage : IStorage {

    public async Task<string> Save(IFormFile fileData) {
        var filePath = Path.GetTempFileName();
            
        await SaveFileData(fileData, filePath);
            
        //await SaveStream(fileData.OpenReadStream(), filePath);
            
        return filePath;
    }

    //private async Task SaveStream(Stream stream, string filePath)
    //{
    //    using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
    //    {
    //        await stream.CopyToAsync(fileStream);
    //    }
    //}

    private async Task SaveFileData(IFormFile fileData, string filePath)
    {
        using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
        {
            await fileData.CopyToAsync(fileStream);
        }
    }

    public void Delete(string filePath) {
        throw new NotImplementedException();
    }
}