using Amazon;
using Amazon.Lambda.AspNetCoreServer;
using Amazon.Lambda.AspNetCoreServer.Hosting;
using Amazon.S3;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
var storageMode = builder.Configuration["PlanningStorageMode"] ?? "local-sqlite";
var planningDbPath = builder.Configuration["PlanningDbPath"]
    ?? (storageMode.Equals("s3-sqlite", StringComparison.OrdinalIgnoreCase)
        ? Path.Combine(Path.GetTempPath(), "sales-planning-demo", "planning.db")
        : Path.Combine(builder.Environment.ContentRootPath, "App_Data", "planning.db"));
var corsAllowedOrigins = (builder.Configuration["CorsAllowedOrigins"] ?? "http://localhost:5173,https://localhost:5173")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AWS_LAMBDA_FUNCTION_NAME")))
{
    builder.Services.AddAWSLambdaHosting(LambdaEventSource.HttpApi, options =>
    {
        options.RegisterResponseContentEncodingForContentType(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ResponseContentEncoding.Base64);
    });
}

builder.Services.AddSingleton<IPlanningRepository>(serviceProvider =>
{
    if (!storageMode.Equals("s3-sqlite", StringComparison.OrdinalIgnoreCase))
    {
        return new SqlitePlanningRepository(planningDbPath);
    }

    var bucketName = builder.Configuration["PlanningStorageS3Bucket"]
        ?? throw new InvalidOperationException("PlanningStorageS3Bucket is required when PlanningStorageMode is s3-sqlite.");
    var objectKey = builder.Configuration["PlanningStorageS3ObjectKey"] ?? "planning/planning.db";
    var regionName = builder.Configuration["PlanningStorageS3Region"] ?? "ap-southeast-5";
    var logger = serviceProvider.GetRequiredService<ILogger<S3BackedSqlitePlanningRepository>>();
    var s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(regionName));

    return new S3BackedSqlitePlanningRepository(s3Client, logger, bucketName, objectKey, planningDbPath);
});
builder.Services.AddSingleton<ISplashAllocator, SplashAllocator>();
builder.Services.AddSingleton<IPlanningService, PlanningService>();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        if (corsAllowedOrigins.Length == 1 && corsAllowedOrigins[0] == "*")
        {
            policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
            return;
        }

        policy.WithOrigins(corsAllowedOrigins).AllowAnyHeader().AllowAnyMethod();
    });
});

var app = builder.Build();

app.UseMiddleware<ApiExceptionHandlingMiddleware>();
app.UseSwagger();
app.UseSwaggerUI();
app.UseCors();
app.MapControllers();
app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    storageMode
}));

app.Run();
