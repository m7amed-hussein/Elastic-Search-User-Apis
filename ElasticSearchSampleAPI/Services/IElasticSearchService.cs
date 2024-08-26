using ElasticSearchSampleAPI.Models;

namespace ElasticSearchSampleAPI.Services;

public interface IElasticSearchService
{
    Task CreateIndexIfNotExistsAsync(string indexName);

    Task<bool> AddOrUpdate(User user);

    Task<bool> AddOrUpdateBulk(IEnumerable<User> users, string indexName);

    Task<User> Get(string key);

    Task<List<User>> GetAll();

    Task<bool> Remove(string key);

    Task<long?> RemoveAll();
}