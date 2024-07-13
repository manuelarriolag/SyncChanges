using Microsoft.AspNetCore.Mvc;
using NLog;
using SyncChanges.WebApi.Model;
using SyncChanges.WebApi.Services;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace SyncChanges.WebApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BrokerController : ControllerBase
    {
        static readonly Logger Log = LogManager.GetCurrentClassLogger();

        private readonly IFileService _uploadService;

        public BrokerController(IFileService uploadService)
        {
            _uploadService = uploadService;
        }

        /// <summary>
        /// Single File Upload
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        // POST: api/<BrokerController>/PostSingleFile
        [HttpPost("PostSingleFile")]
        public async Task<ActionResult> PostSingleFile([FromForm] FileUploadModel fileDetails)
        {
            if (fileDetails == null || fileDetails.FileDetails.Length == 0)
            {
                Log.Warn("fileDetails is null or empty");
                return BadRequest();
            }

            try
            {
                string filePath = await _uploadService.PostFileAsync(fileDetails.FileDetails, fileDetails.FileType);

                long size = fileDetails.FileDetails.Length;

                Log.Info($"Saved to filePath: {filePath}, size: {size}");

                return Ok(new { count = 1, size });

            } catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Multiple File Upload
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        // POST: api/<BrokerController/PostMultipleFile>
        [HttpPost("PostMultipleFile")]
        public async Task<ActionResult> PostMultipleFile([FromForm] List<FileUploadModel> fileDetails)
        {
            if (fileDetails == null || fileDetails.Count == 0)
            {
                Log.Warn("fileDetails is null or empty");
                return BadRequest();
            }

            try
            {
                string[] files = await _uploadService.PostMultiFileAsync(fileDetails);
                long size = fileDetails.Select(s => s.FileDetails).Sum(f => f.Length);

                if (Log.IsDebugEnabled) {
                    foreach (string filePath in files) {
                        Log.Info($"Saved to filePath: {filePath}");
                    }
                }

                Log.Info($"Saved files count: {fileDetails.Count}, totalSize: {size}");


                return Ok(new { count = fileDetails.Count, size });

            } catch (Exception)
            {
                throw;
            }
        }



        //// GET: api/<BrokerController>
        //[HttpGet]
        //public IEnumerable<string> Get()
        //{
        //    return new string[] { "value1", "value2" };
        //}

        //// GET api/<BrokerController>/5
        //[HttpGet("{id}")]
        //public string Get(int id)
        //{
        //    return "value";
        //}

        //// POST api/<BrokerController>
        //[HttpPost]
        //public void Post([FromBody] string value)
        //{
        //}

        //// PUT api/<BrokerController>/5
        //[HttpPut("{id}")]
        //public void Put(int id, [FromBody] string value)
        //{
        //}

        //// DELETE api/<BrokerController>/5
        //[HttpDelete("{id}")]
        //public void Delete(int id)
        //{
        //}



    }
}
