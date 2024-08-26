using ElasticSearchSampleAPI.Models;
using ElasticSearchSampleAPI.Services;
using Microsoft.AspNetCore.Mvc;

namespace ElasticSearchSampleAPI.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UsersController : ControllerBase
{
    private readonly IElasticSearchService _elasticSearchService;
    public UsersController(IElasticSearchService elasticSearchService)
    {
        _elasticSearchService = elasticSearchService;
    }

    [HttpPost("create-index")]
    public async Task<IActionResult> CreateIndex([FromBody] string indexName)
    {
        await _elasticSearchService.CreateIndexIfNotExistsAsync(indexName);
        return Ok();
    }

    [HttpPost]
    public async Task<IActionResult> AddOrUpdate([FromBody] User user)
    {
        var result = await _elasticSearchService.AddOrUpdate(user);
        if (result) return Ok();
        return BadRequest("Failed to add or update the user.");
    }

    [HttpPost("bulk")]
    public async Task<IActionResult> AddOrUpdateBulk([FromBody] IEnumerable<User> users, [FromQuery] string indexName)
    {
        var result = await _elasticSearchService.AddOrUpdateBulk(users, indexName);
        if (result) return Ok();
        return BadRequest("Failed to add or update the users.");
    }

    [HttpGet("{key}")]
    public async Task<IActionResult> Get(string key)
    {
        var user = await _elasticSearchService.Get(key);
        if (user != null) return Ok(user);
        return NotFound();
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var users = await _elasticSearchService.GetAll();
        return Ok(users);
    }

    [HttpDelete("{key}")]
    public async Task<IActionResult> Remove(string key)
    {
        var result = await _elasticSearchService.Remove(key);
        if (result) return Ok();
        return BadRequest("Failed to remove the user.");
    }

    [HttpDelete("all")]
    public async Task<IActionResult> RemoveAll()
    {
        var count = await _elasticSearchService.RemoveAll();
        if (count.HasValue) return Ok($"{count.Value} users removed.");
        return BadRequest("Failed to remove all users.");
    }
}