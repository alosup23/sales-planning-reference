using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
var planningDbPath = builder.Configuration["PlanningDbPath"]
    ?? Path.Combine(builder.Environment.ContentRootPath, "App_Data", "planning.db");

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IPlanningRepository>(_ => new SqlitePlanningRepository(planningDbPath));
builder.Services.AddSingleton<ISplashAllocator, SplashAllocator>();
builder.Services.AddSingleton<IPlanningService, PlanningService>();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .WithOrigins("http://localhost:5173", "https://localhost:5173")
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();

app.UseMiddleware<ApiExceptionHandlingMiddleware>();
app.UseSwagger();
app.UseSwaggerUI();
app.UseCors();
app.MapControllers();

app.Run();
