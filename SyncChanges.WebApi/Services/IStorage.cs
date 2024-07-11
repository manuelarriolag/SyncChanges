namespace SyncChanges.WebApi.Services;

public interface IStorage {
        
    Task<string> Save(IFormFile fileData);
    void Delete(string filePath);

}