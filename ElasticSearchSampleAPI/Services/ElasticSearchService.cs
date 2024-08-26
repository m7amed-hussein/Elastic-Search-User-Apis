using Elastic.Clients.Elasticsearch;
using ElasticSearchSampleAPI.Configuration;
using ElasticSearchSampleAPI.Models;
using Microsoft.Extensions.Options;

namespace ElasticSearchSampleAPI.Services;

public class ElasticSearchService : IElasticSearchService
{
    private readonly ElasticsearchClient _client;
    private readonly ElasticSettings _settings;

    public ElasticSearchService(IOptions<ElasticSettings> opotions)
    {
        _settings = opotions.Value;

        var settings = new ElasticsearchClientSettings(new Uri(_settings.Url))
            // .Authentication()
            .DefaultIndex(_settings.DefaultIndex);

        _client = new ElasticsearchClient(settings);

    }
    
    public async Task CreateIndexIfNotExistsAsync(string indexName)
    {
        if (!_client.Indices.Exists(indexName).Exists)
            await _client.Indices.CreateAsync(indexName);
    }

    public async Task<bool> AddOrUpdate(User user)
    {
        var response = await _client.IndexAsync(user, idx =>
            idx.Index(_settings.DefaultIndex)
                .OpType(OpType.Index));
        return response.IsValidResponse;
    }

    public async Task<bool> AddOrUpdateBulk(IEnumerable<User> users, string indexName)
    {
        var responses = await _client.BulkAsync(
            b => b.Index(_settings.DefaultIndex)
            .UpdateMany(users, 
                (ud, u) 
                    => ud.Doc(u).DocAsUpsert(true)));

        return responses.IsValidResponse;
    }

    public async Task<User> Get(string key)
    {
        var response = await _client.GetAsync<User>(key, g =>
            g.Index(_settings.DefaultIndex));
        return response.Source;
    }

    public async Task<List<User>> GetAll()
    {
        var response = await _client.SearchAsync<User>(s => s.Index(_settings.DefaultIndex));
        return response.Documents.ToList();
    }

    public async Task<bool> Remove(string key)
    {
        var response = await _client.DeleteAsync<User>(key,
            d => d.Index(_settings.DefaultIndex));
        return response.IsValidResponse;
    }

    public async Task<long?> RemoveAll()
    {
        var response = await _client.DeleteByQueryAsync<User>(d => d.Indices(_settings.DefaultIndex));
        return response.IsValidResponse ? response.Deleted : default;
    }
}