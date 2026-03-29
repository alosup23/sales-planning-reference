using Amazon;
using Amazon.Lambda.AspNetCoreServer;
using Amazon.Lambda.AspNetCoreServer.Hosting;
using Amazon.S3;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure;
using SalesPlanning.Api.Infrastructure.Postgres;
using SalesPlanning.Api.Infrastructure.Security;

var builder = WebApplication.CreateBuilder(args);
var storageMode = builder.Configuration["PlanningStorageMode"] ?? "local-sqlite";
var securityMode = builder.Configuration["PlanningSecurityMode"] ?? "entra";
var authEnabled = !string.Equals(securityMode, "disabled", StringComparison.OrdinalIgnoreCase);
var planningDbPath = builder.Configuration["PlanningDbPath"]
    ?? (storageMode.Equals("s3-sqlite", StringComparison.OrdinalIgnoreCase) || storageMode.Equals("postgres", StringComparison.OrdinalIgnoreCase)
        ? Path.Combine(Path.GetTempPath(), "sales-planning-demo", "planning.db")
        : Path.Combine(builder.Environment.ContentRootPath, "App_Data", "planning.db"));
var corsAllowedOrigins = (builder.Configuration["CorsAllowedOrigins"] ?? "http://localhost:5173,https://localhost:5173,https://d22xc0mfhkv9bk.cloudfront.net")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var entraTenantId = builder.Configuration["EntraTenantId"] ?? "76ad236c-6db1-4d3d-9901-996450816c3c";
var entraClientId = builder.Configuration["EntraClientId"] ?? "557f0c81-0531-4616-b62e-0b69eb7cb86f";
var entraApiAudience = builder.Configuration["EntraApiAudience"] ?? $"api://{entraClientId}";
var entraApiScope = builder.Configuration["EntraApiScope"] ?? "SalesPlanning.Access";

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
    if (storageMode.Equals("postgres", StringComparison.OrdinalIgnoreCase))
    {
        var postgresLogger = serviceProvider.GetRequiredService<ILogger<PostgresBackedSqlitePlanningRepository>>();
        var connectionString = PostgresConnectionStringResolver.ResolveAsync(builder.Configuration, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
        var migrationsDirectory = Path.Combine(builder.Environment.ContentRootPath, "Infrastructure", "Postgres", "Migrations");
        var applyMigrationsOnStartup = bool.TryParse(builder.Configuration["PlanningApplyMigrationsOnStartup"], out var parsedApplyMigrations)
            ? parsedApplyMigrations
            : true;
        return new PostgresBackedSqlitePlanningRepository(
            connectionString,
            postgresLogger,
            planningDbPath,
            applyMigrationsOnStartup,
            migrationsDirectory,
            builder.Configuration["PlanningBootstrapS3Bucket"],
            builder.Configuration["PlanningBootstrapS3ObjectKey"],
            builder.Configuration["PlanningBootstrapS3Region"],
            builder.Configuration["PlanningBootstrapSeedKey"]);
    }

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

if (authEnabled)
{
    builder.Services
        .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            options.MapInboundClaims = false;
            options.Authority = $"https://login.microsoftonline.com/{entraTenantId}/v2.0";
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true,
                ValidIssuers =
                [
                    $"https://login.microsoftonline.com/{entraTenantId}/v2.0",
                    $"https://sts.windows.net/{entraTenantId}/"
                ],
                ValidateAudience = true,
                ValidAudience = entraApiAudience,
                ValidateLifetime = true,
                NameClaimType = "name",
                RoleClaimType = "roles",
                ValidTypes = ["JWT", "at+jwt"]
            };
        });
    builder.Services.AddAuthorization(options =>
    {
        options.AddPolicy(EntraApiAccessPolicy.PolicyName, EntraApiAccessPolicy.Build(entraApiScope));
    });
}

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        if (builder.Environment.IsDevelopment() && corsAllowedOrigins.Length == 1 && corsAllowedOrigins[0] == "*")
        {
            policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
            return;
        }

        policy.WithOrigins(corsAllowedOrigins).AllowAnyHeader().AllowAnyMethod();
    });
});

var app = builder.Build();

app.UseMiddleware<ApiExceptionHandlingMiddleware>();
if (builder.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.UseCors();
if (authEnabled)
{
    app.UseAuthentication();
    app.UseAuthorization();
}
var controllers = app.MapControllers();
if (authEnabled)
{
    controllers.RequireAuthorization(EntraApiAccessPolicy.PolicyName);
}
app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    storageMode,
    authEnabled
}));

app.Run();
