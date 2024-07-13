namespace SyncChanges.WebApi.Services;

public class FileSystemStorage : IStorage {

    public async Task<string> Save(IFormFile fileData) {

        // Establecer la ruta del archivo
        var filePath = Path.Combine(ExtensionMethods.GetDirectoryName(Constants.INPUT_BOX), Path.GetFileName(fileData.FileName));

        // Establecer el nombre del archivo
        filePath = filePath.Replace(Constants.CHANGEINFO_TO_SEND, Constants.CHANGEINFO_RECEIVED);

        // Grabar el archivo
        await SaveFileData(fileData, filePath);
            
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