namespace SalesPlanning.Api.Contracts;

public sealed record UndoRedoAvailabilityDto(
    bool CanUndo,
    bool CanRedo,
    int UndoDepth,
    int RedoDepth,
    int Limit);

public sealed record UndoPlanningActionResponse(
    string Status,
    UndoRedoAvailabilityDto Availability);

public sealed record RedoPlanningActionResponse(
    string Status,
    UndoRedoAvailabilityDto Availability);
