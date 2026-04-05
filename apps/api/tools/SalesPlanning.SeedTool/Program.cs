using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure.Postgres;

if (args.Length < 2)
{
    Console.WriteLine("Usage: SalesPlanning.SeedTool <storeWorkbookPath> <productWorkbookPath>");
    Console.WriteLine("Connection is resolved from PlanningPostgresConnectionString or PlanningPostgresSecretArn.");
    return 1;
}

var storeWorkbookPath = args[0];
var productWorkbookPath = args[1];

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

var configuration = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .Build();
var connectionString = await PostgresConnectionStringResolver.ResolveAsync(configuration, CancellationToken.None);
var migrationsDirectory = Path.GetFullPath(
    Path.Combine(
        AppContext.BaseDirectory,
        "../../../../src/SalesPlanning.Api/Infrastructure/Postgres/Migrations"));
var repository = new PostgresPlanningRepository(
    connectionString,
    NullLogger<PostgresPlanningRepository>.Instance,
    true,
    migrationsDirectory);
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
