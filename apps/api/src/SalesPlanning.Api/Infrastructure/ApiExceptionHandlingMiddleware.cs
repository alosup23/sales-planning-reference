using Microsoft.AspNetCore.Mvc;

namespace SalesPlanning.Api.Infrastructure;

public sealed class ApiExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ApiExceptionHandlingMiddleware> _logger;

    public ApiExceptionHandlingMiddleware(RequestDelegate next, ILogger<ApiExceptionHandlingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Unhandled planning API exception for {Path}", context.Request.Path);
            await WriteProblemDetailsAsync(context, exception);
        }
    }

    private static async Task WriteProblemDetailsAsync(HttpContext context, Exception exception)
    {
        var (statusCode, title) = exception switch
        {
            InvalidOperationException invalidOperation when invalidOperation.Message.Contains("changed since it was last read", StringComparison.OrdinalIgnoreCase)
                => (StatusCodes.Status409Conflict, "Planning conflict"),
            InvalidOperationException => (StatusCodes.Status400BadRequest, "Planning validation failed"),
            KeyNotFoundException => (StatusCodes.Status404NotFound, "Planning resource not found"),
            _ => (StatusCodes.Status500InternalServerError, "Unexpected server error")
        };

        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/problem+json";

        var problem = new ProblemDetails
        {
            Status = statusCode,
            Title = title,
            Detail = statusCode >= 500
                ? "An unexpected server error occurred. Contact support if the issue persists."
                : exception.Message,
            Instance = context.Request.Path
        };

        await context.Response.WriteAsJsonAsync(problem);
    }
}
