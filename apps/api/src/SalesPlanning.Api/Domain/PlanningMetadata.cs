namespace SalesPlanning.Api.Domain;

public sealed record ProductNode(
    long ProductNodeId,
    long StoreId,
    long? ParentProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf,
    string NodeKind,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId);

public sealed record StoreNodeMetadata(
    long StoreId,
    string StoreLabel,
    string ClusterLabel,
    string RegionLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    string? StoreCode = null,
    string? State = null,
    decimal? Latitude = null,
    decimal? Longitude = null,
    string? OpeningDate = null,
    string? Sssg = null,
    string? SalesType = null,
    string? Status = null,
    string? Storey = null,
    string? BuildingStatus = null,
    decimal? Gta = null,
    decimal? Nta = null,
    string? Rsom = null,
    string? Dm = null,
    decimal? Rental = null,
    bool IsActive = true);

public sealed record StoreProfileOptionValue(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record ProductProfileMetadata(
    string SkuVariant,
    string Description,
    string? Description2,
    decimal Price,
    decimal Cost,
    string DptNo,
    string ClssNo,
    string? BrandNo,
    string Department,
    string Class,
    string? Brand,
    string? RevDepartment,
    string? RevClass,
    string Subclass,
    string? ProdGroup,
    string? ProdType,
    string? ActiveFlag,
    string? OrderFlag,
    string? BrandType,
    string? LaunchMonth,
    string? Gender,
    string? Size,
    string? Collection,
    string? Promo,
    string? RamadhanPromo,
    bool IsActive);

public sealed record ProductProfileOptionValue(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record ProductHierarchyCatalogRecord(
    string DptNo,
    string ClssNo,
    string Department,
    string Class,
    string ProdGroup,
    bool IsActive);

public sealed record ProductSubclassCatalogRecord(
    string Department,
    string Class,
    string Subclass,
    bool IsActive);

public sealed record TimePeriodNode(long TimePeriodId, long? ParentTimePeriodId, string Label, string Grain, int SortOrder);

public sealed record PlanningMetadataSnapshot(
    IReadOnlyDictionary<long, ProductNode> ProductNodes,
    IReadOnlyDictionary<long, TimePeriodNode> TimePeriods,
    IReadOnlyDictionary<long, StoreNodeMetadata> Stores);
