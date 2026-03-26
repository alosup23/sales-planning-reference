using Amazon;
using Amazon.S3;
using Microsoft.Extensions.Logging.Abstractions;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure;

if (args.Length < 5)
{
    Console.WriteLine("Usage: SalesPlanning.SeedTool <storeWorkbookPath> <productWorkbookPath> <bucketName> <objectKey> <regionSystemName>");
    return 1;
}

var storeWorkbookPath = args[0];
var productWorkbookPath = args[1];
var bucketName = args[2];
var objectKey = args[3];
var region = RegionEndpoint.GetBySystemName(args[4]);

if (!File.Exists(storeWorkbookPath))
{
    Console.Error.WriteLine($"Store workbook not found: {storeWorkbookPath}");
    return 2;
}

if (!File.Exists(productWorkbookPath))
{
    Console.Error.WriteLine($"Product workbook not found: {productWorkbookPath}");
    return 3;
}

var localDbPath = Path.Combine(Path.GetTempPath(), $"sales-planning-seed-{Guid.NewGuid():N}.db");
try
{
    using var s3Client = new AmazonS3Client(region);
    var repository = new S3BackedSqlitePlanningRepository(
        s3Client,
        NullLogger<S3BackedSqlitePlanningRepository>.Instance,
        bucketName,
        objectKey,
        localDbPath);
    var service = new PlanningService(repository, new SplashAllocator());

    var existingStores = await repository.GetStoresAsync(CancellationToken.None);
    foreach (var store in existingStores)
    {
        await repository.DeleteStoreProfileAsync(1, store.StoreId, CancellationToken.None);
    }

    await using (var storeStream = File.OpenRead(storeWorkbookPath))
    {
        var storeResult = await service.ImportStoreProfilesAsync(storeStream, Path.GetFileName(storeWorkbookPath), CancellationToken.None);
        Console.WriteLine($"Store import applied: processed={storeResult.RowsProcessed}, added={storeResult.StoresAdded}, updated={storeResult.StoresUpdated}");
    }

    await using (var productStream = File.OpenRead(productWorkbookPath))
    {
        var productResult = await service.ImportProductProfilesAsync(productStream, Path.GetFileName(productWorkbookPath), CancellationToken.None);
        Console.WriteLine($"Product import applied: processed={productResult.RowsProcessed}, added={productResult.ProductsAdded}, updated={productResult.ProductsUpdated}, hierarchy={productResult.HierarchyRowsProcessed}");
    }

    await repository.RebuildPlanningFromMasterDataAsync(1, 2026, CancellationToken.None);
    Console.WriteLine("FY26 planning cube rebuilt from store and product master data.");
    return 0;
}
finally
{
    try
    {
        if (File.Exists(localDbPath))
        {
            File.Delete(localDbPath);
        }
    }
    catch
    {
    }
}
