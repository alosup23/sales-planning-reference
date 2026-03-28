namespace SalesPlanning.Api.Contracts;

public sealed record SeasonalityEventProfileDto(
    long? SeasonalityEventProfileId,
    string? Department,
    string? Class,
    string? Subclass,
    string? SeasonCode,
    string? EventCode,
    int Month,
    decimal Weight,
    string? PromoWindow,
    bool PeakFlag,
    bool IsActive);

public sealed record SeasonalityEventProfileResponse(
    IReadOnlyList<SeasonalityEventProfileDto> Profiles,
    int TotalCount,
    int PageNumber,
    int PageSize,
    string? SearchTerm);

public sealed record UpsertSeasonalityEventProfileRequest(
    long? SeasonalityEventProfileId,
    string? Department,
    string? Class,
    string? Subclass,
    string? SeasonCode,
    string? EventCode,
    int Month,
    decimal Weight,
    string? PromoWindow,
    bool PeakFlag,
    bool IsActive);

public sealed record DeleteSeasonalityEventProfileRequest(long SeasonalityEventProfileId);

public sealed record InactivateSeasonalityEventProfileRequest(long SeasonalityEventProfileId);

public sealed record SeasonalityEventProfileImportResponse(
    int RowsProcessed,
    int RecordsAdded,
    int RecordsUpdated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);
