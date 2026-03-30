using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public sealed class PlanningAsyncJobManager : BackgroundService, IPlanningAsyncJobManager
{
    private readonly Channel<QueuedPlanningAsyncJob> _channel = Channel.CreateUnbounded<QueuedPlanningAsyncJob>();
    private readonly ConcurrentDictionary<string, PlanningAsyncJobState> _jobs = new(StringComparer.OrdinalIgnoreCase);

    public Task<AsyncJobStatusResponse> EnqueueAsync(
        string category,
        string operation,
        string requestedBy,
        Func<IProgress<PlanningAsyncJobProgress>, CancellationToken, Task<PlanningAsyncJobExecutionResult>> work,
        CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid().ToString("N");
        var createdAt = DateTimeOffset.UtcNow;
        var state = new PlanningAsyncJobState(
            JobId: jobId,
            Category: category,
            Operation: operation,
            RequestedBy: requestedBy,
            Status: "queued",
            ProgressPercent: 0,
            ProgressMessage: "Queued",
            CreatedAt: createdAt,
            StartedAt: null,
            CompletedAt: null,
            ErrorMessage: null,
            Summary: null,
            DownloadContent: null,
            DownloadFileName: null,
            DownloadContentType: null);

        _jobs[jobId] = state;
        if (!_channel.Writer.TryWrite(new QueuedPlanningAsyncJob(jobId, work)))
        {
            throw new InvalidOperationException("Unable to queue the async job.");
        }

        return Task.FromResult(ToResponse(state));
    }

    public Task<AsyncJobStatusResponse> GetStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        if (!_jobs.TryGetValue(jobId, out var state))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        return Task.FromResult(ToResponse(state));
    }

    public Task<(byte[] Content, string FileName, string ContentType)> DownloadResultAsync(string jobId, CancellationToken cancellationToken)
    {
        if (!_jobs.TryGetValue(jobId, out var state))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        if (state.DownloadContent is null || string.IsNullOrWhiteSpace(state.DownloadFileName) || string.IsNullOrWhiteSpace(state.DownloadContentType))
        {
            throw new InvalidOperationException("This async job does not have a downloadable result.");
        }

        return Task.FromResult((state.DownloadContent, state.DownloadFileName, state.DownloadContentType));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var queuedJob in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            if (!_jobs.TryGetValue(queuedJob.JobId, out var currentState))
            {
                continue;
            }

            UpdateState(currentState with
            {
                Status = "running",
                ProgressPercent = 5,
                ProgressMessage = "Starting",
                StartedAt = DateTimeOffset.UtcNow
            });

            var progress = new Progress<PlanningAsyncJobProgress>(update =>
            {
                if (_jobs.TryGetValue(queuedJob.JobId, out var runningState))
                {
                    UpdateState(runningState with
                    {
                        ProgressPercent = Math.Clamp(update.ProgressPercent, 0, 100),
                        ProgressMessage = string.IsNullOrWhiteSpace(update.ProgressMessage) ? runningState.ProgressMessage : update.ProgressMessage
                    });
                }
            });

            try
            {
                var result = await queuedJob.Work(progress, stoppingToken);
                if (_jobs.TryGetValue(queuedJob.JobId, out var finishedState))
                {
                    UpdateState(finishedState with
                    {
                        Status = "completed",
                        ProgressPercent = 100,
                        ProgressMessage = "Completed",
                        CompletedAt = DateTimeOffset.UtcNow,
                        Summary = result.Summary,
                        DownloadContent = result.DownloadContent,
                        DownloadFileName = result.DownloadFileName,
                        DownloadContentType = result.DownloadContentType
                    });
                }
            }
            catch (Exception exception)
            {
                if (_jobs.TryGetValue(queuedJob.JobId, out var failedState))
                {
                    UpdateState(failedState with
                    {
                        Status = "failed",
                        ProgressMessage = "Failed",
                        CompletedAt = DateTimeOffset.UtcNow,
                        ErrorMessage = exception.Message
                    });
                }
            }
        }
    }

    private void UpdateState(PlanningAsyncJobState state)
    {
        _jobs[state.JobId] = state;
    }

    private static AsyncJobStatusResponse ToResponse(PlanningAsyncJobState state)
    {
        return new AsyncJobStatusResponse(
            state.JobId,
            state.Category,
            state.Operation,
            state.Status,
            state.ProgressPercent,
            state.ProgressMessage,
            state.CreatedAt,
            state.StartedAt,
            state.CompletedAt,
            state.ErrorMessage,
            state.Summary,
            state.DownloadContent is not null,
            state.DownloadFileName,
            state.DownloadContentType);
    }

    private sealed record QueuedPlanningAsyncJob(
        string JobId,
        Func<IProgress<PlanningAsyncJobProgress>, CancellationToken, Task<PlanningAsyncJobExecutionResult>> Work);

    private sealed record PlanningAsyncJobState(
        string JobId,
        string Category,
        string Operation,
        string RequestedBy,
        string Status,
        int ProgressPercent,
        string ProgressMessage,
        DateTimeOffset CreatedAt,
        DateTimeOffset? StartedAt,
        DateTimeOffset? CompletedAt,
        string? ErrorMessage,
        AsyncJobSummaryDto? Summary,
        byte[]? DownloadContent,
        string? DownloadFileName,
        string? DownloadContentType);
}
