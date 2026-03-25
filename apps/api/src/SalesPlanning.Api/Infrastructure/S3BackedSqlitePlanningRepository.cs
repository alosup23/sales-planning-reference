using Amazon.S3;
using Amazon.S3.Model;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed class S3BackedSqlitePlanningRepository : IPlanningRepository
{
    private readonly SqlitePlanningRepository _innerRepository;
    private readonly IAmazonS3 _s3Client;
    private readonly ILogger<S3BackedSqlitePlanningRepository> _logger;
    private readonly string _bucketName;
    private readonly string _objectKey;
    private readonly string _localDatabasePath;
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private bool _hydrated;

    public S3BackedSqlitePlanningRepository(
        IAmazonS3 s3Client,
        ILogger<S3BackedSqlitePlanningRepository> logger,
        string bucketName,
        string objectKey,
        string localDatabasePath)
    {
        _s3Client = s3Client;
        _logger = logger;
        _bucketName = bucketName;
        _objectKey = objectKey;
        _localDatabasePath = localDatabasePath;
        _innerRepository = new SqlitePlanningRepository(localDatabasePath);
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetMetadataAsync, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetCellsAsync(coordinates, ct), cancellationToken);

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetCellAsync(coordinate, ct), cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetScenarioCellsAsync(scenarioVersionId, ct), cancellationToken);

    public Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertCellsAsync(cells, ct), cancellationToken);

    public Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.AppendAuditAsync(audit, ct), cancellationToken);

    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetNextActionIdAsync, cancellationToken);

    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, ct), cancellationToken);

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetGridSliceAsync(scenarioVersionId, ct), cancellationToken);

    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.AddRowAsync(request, ct), cancellationToken);

    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteRowAsync(scenarioVersionId, productNodeId, ct), cancellationToken);

    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, ct), cancellationToken);

    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.EnsureYearAsync(scenarioVersionId, fiscalYear, ct), cancellationToken);

    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoresAsync, cancellationToken);

    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetHierarchyMappingsAsync, cancellationToken);

    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchyDepartmentAsync(departmentLabel, ct), cancellationToken);

    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchyClassAsync(departmentLabel, classLabel, ct), cancellationToken);

    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchySubclassAsync(departmentLabel, classLabel, subclassLabel, ct), cancellationToken);

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.FindProductNodeByPathAsync(path, ct), cancellationToken);

    public Task ResetAsync(CancellationToken cancellationToken) =>
        WithMutationAsync(_innerRepository.ResetAsync, cancellationToken);

    private async Task<T> WithReadAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        return await action(cancellationToken);
    }

    private async Task<T> WithMutationAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            var result = await action(cancellationToken);
            await UploadLocalDatabaseAsync(cancellationToken);
            return result;
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task WithMutationAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            await action(cancellationToken);
            await UploadLocalDatabaseAsync(cancellationToken);
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task EnsureHydratedAsync(CancellationToken cancellationToken)
    {
        if (_hydrated)
        {
            return;
        }

        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            if (_hydrated)
            {
                return;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(_localDatabasePath) ?? Path.GetTempPath());
            if (await ObjectExistsAsync(cancellationToken))
            {
                using var response = await _s3Client.GetObjectAsync(_bucketName, _objectKey, cancellationToken);
                await response.WriteResponseStreamToFileAsync(_localDatabasePath, false, cancellationToken);
                _logger.LogInformation("Hydrated planning database from s3://{Bucket}/{Key}", _bucketName, _objectKey);
            }
            else
            {
                _logger.LogInformation(
                    "No planning database object found at s3://{Bucket}/{Key}. Using local seeded database and uploading on first mutation.",
                    _bucketName,
                    _objectKey);
            }

            _hydrated = true;
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task UploadLocalDatabaseAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_localDatabasePath))
        {
            throw new FileNotFoundException("Local planning database was not found for upload.", _localDatabasePath);
        }

        await using var stream = File.Open(_localDatabasePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await _s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = _objectKey,
            InputStream = stream
        }, cancellationToken);

        _logger.LogInformation("Uploaded planning database to s3://{Bucket}/{Key}", _bucketName, _objectKey);
    }

    private async Task<bool> ObjectExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _s3Client.GetObjectMetadataAsync(_bucketName, _objectKey, cancellationToken);
            return true;
        }
        catch (AmazonS3Exception exception) when (exception.StatusCode == System.Net.HttpStatusCode.NotFound || exception.ErrorCode is "NoSuchKey" or "NotFound")
        {
            return false;
        }
    }
}
