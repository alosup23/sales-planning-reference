using System.Globalization;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    private const string StoreRootViewRowId = "view:store:root";
    private const string DepartmentRootViewRowId = "view:department:root";

    public async Task<PlanningDepartmentScopeResponse> GetPlanningDepartmentScopesAsync(CancellationToken cancellationToken)
    {
        var departments = (await _repository.GetHierarchyMappingsAsync(cancellationToken))
            .Select(department => department.DepartmentLabel)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(label => label, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return new PlanningDepartmentScopeResponse(departments);
    }

    public async Task<GridSliceResponse> GetGridViewRootAsync(PlanningGridViewRequest request, CancellationToken cancellationToken)
    {
        if (string.Equals(request.View, "department", StringComparison.OrdinalIgnoreCase))
        {
            return await BuildDepartmentRootSliceAsync(request, cancellationToken);
        }

        return await BuildStoreRootSliceAsync(request, cancellationToken);
    }

    public async Task<GridViewBlockResponse> GetGridViewChildrenAsync(PlanningGridViewRequest request, string parentViewRowId, CancellationToken cancellationToken)
    {
        var rows = string.Equals(request.View, "department", StringComparison.OrdinalIgnoreCase)
            ? await BuildDepartmentChildrenAsync(request, parentViewRowId, cancellationToken)
            : await BuildStoreChildrenAsync(request, parentViewRowId, cancellationToken);

        return new GridViewBlockResponse(request.ScenarioVersionId, parentViewRowId, rows);
    }

    private async Task<GridSliceResponse> BuildStoreRootSliceAsync(PlanningGridViewRequest request, CancellationToken cancellationToken)
    {
        var canonical = await _repository.GetGridSliceAsync(request.ScenarioVersionId, request.SelectedStoreId, null, null, false, cancellationToken);
        var storeRows = canonical.Rows
            .Select(row => DecorateCanonicalRow(row, "store", row.Path, row.Label, row.Level + 1, row.NodeKind))
            .Where(row => string.Equals(row.StructureRole, "store", StringComparison.OrdinalIgnoreCase))
            .ToList();

        var rootRows = storeRows.Count == 0
            ? Array.Empty<GridRowDto>()
            : [CreateSyntheticGridRow(canonical, StoreRootViewRowId, 0, -10, "Store Total", ["Store Total"], "store", storeRows)];

        return canonical with { Rows = rootRows };
    }

    private async Task<IReadOnlyList<GridRowDto>> BuildStoreChildrenAsync(PlanningGridViewRequest request, string parentViewRowId, CancellationToken cancellationToken)
    {
        if (string.Equals(parentViewRowId, StoreRootViewRowId, StringComparison.OrdinalIgnoreCase))
        {
            var canonical = await _repository.GetGridSliceAsync(request.ScenarioVersionId, request.SelectedStoreId, null, null, false, cancellationToken);
            return canonical.Rows
                .Select(row => DecorateCanonicalRow(row, "store", ["Store Total", .. row.Path], row.Label, row.Level + 1, row.NodeKind))
                .Where(row => string.Equals(row.StructureRole, "store", StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        var parsed = ParseCanonicalNodeViewRowId(parentViewRowId);
        var branch = await _repository.GetGridBranchRowsAsync(request.ScenarioVersionId, parsed.ProductNodeId, cancellationToken);
        var parentPath = ParsePathFromViewRowId(parentViewRowId);
        return branch.Rows
            .Select(row => DecorateCanonicalRow(
                row,
                "store",
                [.. parentPath, row.Label],
                row.Label,
                parentPath.Length,
                row.NodeKind))
            .ToList();
    }

    private async Task<GridSliceResponse> BuildDepartmentRootSliceAsync(PlanningGridViewRequest request, CancellationToken cancellationToken)
    {
        var canonical = await LoadDepartmentBootstrapSliceAsync(request.ScenarioVersionId, request.SelectedDepartmentLabel, cancellationToken);
        var departmentRows = BuildDepartmentAggregateRows(canonical, request.SelectedDepartmentLabel);
        var rootRows = departmentRows.Count == 0
            ? Array.Empty<GridRowDto>()
            : [CreateSyntheticGridRow(canonical, DepartmentRootViewRowId, 0, -20, "Department Total", ["Department Total"], "department", departmentRows)];
        return canonical with { Rows = rootRows };
    }

    private async Task<IReadOnlyList<GridRowDto>> BuildDepartmentChildrenAsync(PlanningGridViewRequest request, string parentViewRowId, CancellationToken cancellationToken)
    {
        if (string.Equals(parentViewRowId, DepartmentRootViewRowId, StringComparison.OrdinalIgnoreCase))
        {
            var canonical = await LoadDepartmentBootstrapSliceAsync(request.ScenarioVersionId, request.SelectedDepartmentLabel, cancellationToken);
            return BuildDepartmentAggregateRows(canonical, request.SelectedDepartmentLabel);
        }

        if (parentViewRowId.StartsWith("view:department:department:", StringComparison.OrdinalIgnoreCase))
        {
            var departmentLabel = Uri.UnescapeDataString(parentViewRowId["view:department:department:".Length..]);
            return string.Equals(request.DepartmentLayout, "department-class-store", StringComparison.OrdinalIgnoreCase)
                ? await BuildDepartmentClassChildrenAsync(request.ScenarioVersionId, departmentLabel, request.SelectedDepartmentLabel, cancellationToken)
                : await BuildDepartmentStoreChildrenAsync(request.ScenarioVersionId, departmentLabel, request.SelectedDepartmentLabel, cancellationToken);
        }

        if (parentViewRowId.StartsWith("view:department:class:", StringComparison.OrdinalIgnoreCase))
        {
            var payload = parentViewRowId["view:department:class:".Length..].Split(':', 2);
            var departmentLabel = Uri.UnescapeDataString(payload[0]);
            var classLabel = Uri.UnescapeDataString(payload[1]);
            return await BuildDepartmentClassStoreChildrenAsync(request.ScenarioVersionId, departmentLabel, classLabel, request.SelectedDepartmentLabel, cancellationToken);
        }

        var parsed = ParseCanonicalNodeViewRowId(parentViewRowId);
        var branch = await _repository.GetGridBranchRowsAsync(request.ScenarioVersionId, parsed.ProductNodeId, cancellationToken);
        var parentPath = ParsePathFromViewRowId(parentViewRowId);
        return branch.Rows
            .Select(row => DecorateCanonicalRow(
                row,
                "department",
                [.. parentPath, row.Label],
                row.Label,
                parentPath.Length,
                row.NodeKind))
            .ToList();
    }

    private async Task<GridSliceResponse> LoadDepartmentBootstrapSliceAsync(long scenarioVersionId, string? selectedDepartmentLabel, CancellationToken cancellationToken)
    {
        var storeRootIds = (await _repository.GetStoreRootProductNodeIdsAsync(cancellationToken)).Values.ToArray();
        return await _repository.GetGridSliceAsync(scenarioVersionId, null, selectedDepartmentLabel, storeRootIds, false, cancellationToken);
    }

    private List<GridRowDto> BuildDepartmentAggregateRows(GridSliceResponse canonical, string? selectedDepartmentLabel)
    {
        var filteredDepartmentRows = canonical.Rows
            .Where(row => string.Equals(row.NodeKind, "department", StringComparison.OrdinalIgnoreCase))
            .Where(row => string.IsNullOrWhiteSpace(selectedDepartmentLabel) || string.Equals(row.Label, selectedDepartmentLabel, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return filteredDepartmentRows
            .GroupBy(row => row.Label, StringComparer.OrdinalIgnoreCase)
            .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => CreateSyntheticGridRow(
                canonical,
                $"view:department:department:{Uri.EscapeDataString(group.Key)}",
                0,
                -1000 - group.Key.GetHashCode(StringComparison.OrdinalIgnoreCase),
                group.Key,
                ["Department Total", group.Key],
                "department",
                group.ToList()))
            .ToList();
    }

    private async Task<IReadOnlyList<GridRowDto>> BuildDepartmentStoreChildrenAsync(long scenarioVersionId, string departmentLabel, string? selectedDepartmentLabel, CancellationToken cancellationToken)
    {
        var canonical = await LoadDepartmentBootstrapSliceAsync(scenarioVersionId, selectedDepartmentLabel, cancellationToken);
        return canonical.Rows
            .Where(row => string.Equals(row.NodeKind, "department", StringComparison.OrdinalIgnoreCase))
            .Where(row => string.Equals(row.Label, departmentLabel, StringComparison.OrdinalIgnoreCase))
            .OrderBy(row => row.StoreLabel, StringComparer.OrdinalIgnoreCase)
            .Select(row => DecorateCanonicalRow(
                row,
                "department",
                ["Department Total", departmentLabel, row.StoreLabel],
                row.StoreLabel,
                2,
                "store"))
            .ToList();
    }

    private async Task<IReadOnlyList<GridRowDto>> BuildDepartmentClassChildrenAsync(long scenarioVersionId, string departmentLabel, string? selectedDepartmentLabel, CancellationToken cancellationToken)
    {
        var bootstrap = await LoadDepartmentBootstrapSliceAsync(scenarioVersionId, selectedDepartmentLabel, cancellationToken);
        var departmentNodes = bootstrap.Rows
            .Where(row => string.Equals(row.NodeKind, "department", StringComparison.OrdinalIgnoreCase))
            .Where(row => string.Equals(row.Label, departmentLabel, StringComparison.OrdinalIgnoreCase))
            .Select(row => row.ProductNodeId)
            .Distinct()
            .ToArray();

        var canonical = await _repository.GetGridSliceAsync(scenarioVersionId, null, departmentLabel, departmentNodes, false, cancellationToken);
        return canonical.Rows
            .Where(row => string.Equals(row.NodeKind, "class", StringComparison.OrdinalIgnoreCase))
            .GroupBy(row => row.Label, StringComparer.OrdinalIgnoreCase)
            .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => CreateSyntheticGridRow(
                canonical,
                $"view:department:class:{Uri.EscapeDataString(departmentLabel)}:{Uri.EscapeDataString(group.Key)}",
                0,
                -3000 - HashCode.Combine(departmentLabel.ToUpperInvariant(), group.Key.ToUpperInvariant()),
                group.Key,
                ["Department Total", departmentLabel, group.Key],
                "class",
                group.ToList()))
            .ToList();
    }

    private async Task<IReadOnlyList<GridRowDto>> BuildDepartmentClassStoreChildrenAsync(long scenarioVersionId, string departmentLabel, string classLabel, string? selectedDepartmentLabel, CancellationToken cancellationToken)
    {
        var bootstrap = await LoadDepartmentBootstrapSliceAsync(scenarioVersionId, selectedDepartmentLabel, cancellationToken);
        var departmentNodes = bootstrap.Rows
            .Where(row => string.Equals(row.NodeKind, "department", StringComparison.OrdinalIgnoreCase))
            .Where(row => string.Equals(row.Label, departmentLabel, StringComparison.OrdinalIgnoreCase))
            .Select(row => row.ProductNodeId)
            .Distinct()
            .ToArray();

        var canonical = await _repository.GetGridSliceAsync(scenarioVersionId, null, departmentLabel, departmentNodes, false, cancellationToken);
        return canonical.Rows
            .Where(row => string.Equals(row.NodeKind, "class", StringComparison.OrdinalIgnoreCase))
            .Where(row => string.Equals(row.Label, classLabel, StringComparison.OrdinalIgnoreCase))
            .OrderBy(row => row.StoreLabel, StringComparer.OrdinalIgnoreCase)
            .Select(row => DecorateCanonicalRow(
                row,
                "department",
                ["Department Total", departmentLabel, classLabel, row.StoreLabel],
                row.StoreLabel,
                3,
                "store"))
            .ToList();
    }

    private static GridRowDto CreateSyntheticGridRow(
        GridSliceResponse canonical,
        string viewRowId,
        long storeId,
        long productNodeId,
        string label,
        string[] path,
        string structureRole,
        IReadOnlyList<GridRowDto> sourceRows)
    {
        return new GridRowDto(
            storeId,
            productNodeId,
            label,
            path.Length - 1,
            path,
            false,
            "virtual",
            sourceRows.FirstOrDefault()?.StoreLabel ?? "All Stores",
            sourceRows.FirstOrDefault()?.ClusterLabel ?? "Mixed Cluster",
            sourceRows.FirstOrDefault()?.RegionLabel ?? "Mixed Region",
            "synthetic",
            null,
            null,
            null,
            SumCells(sourceRows, canonical),
            viewRowId,
            structureRole,
            sourceRows.Count == 1 ? sourceRows[0].BindingStoreId ?? sourceRows[0].StoreId : null,
            sourceRows.Count == 1 ? sourceRows[0].BindingProductNodeId ?? sourceRows[0].ProductNodeId : null,
            sourceRows.SelectMany(row => row.SplashRoots ?? [new GridScopeRootDto(row.StoreId, row.ProductNodeId)]).Distinct().ToArray());
    }

    private static GridRowDto DecorateCanonicalRow(
        GridRowDto row,
        string viewPrefix,
        string[] path,
        string label,
        int level,
        string structureRole)
    {
        return row with
        {
            Label = label,
            Level = level,
            Path = path,
            ViewRowId = $"view:{viewPrefix}:node:{row.StoreId}:{row.ProductNodeId}:{Uri.EscapeDataString(string.Join(">", path))}",
            StructureRole = structureRole,
            BindingStoreId = row.StoreId,
            BindingProductNodeId = row.ProductNodeId,
            SplashRoots = [new GridScopeRootDto(row.StoreId, row.ProductNodeId)]
        };
    }

    private static Dictionary<long, GridPeriodCellDto> SumCells(IReadOnlyList<GridRowDto> rows, GridSliceResponse data)
    {
        return data.Periods.ToDictionary(
            period => period.TimePeriodId,
            period =>
            {
                var measures = data.Measures.ToDictionary(
                    measure => measure.MeasureId,
                    measure =>
                    {
                        var rawValue = rows.Sum(row => row.Cells.GetValueOrDefault(period.TimePeriodId)?.Measures.GetValueOrDefault(measure.MeasureId)?.Value ?? 0m);
                        var value = measure.MeasureId switch
                        {
                            3 => DeriveAsp(rows, period.TimePeriodId),
                            4 => DeriveUnitCost(rows, period.TimePeriodId),
                            7 => DeriveGrossProfitPercent(rows, period.TimePeriodId),
                            _ => rawValue
                        };

                        return new GridCellDto(
                            value,
                            1m,
                            rows.Count > 0 && rows.All(row => row.Cells.GetValueOrDefault(period.TimePeriodId)?.Measures.GetValueOrDefault(measure.MeasureId)?.IsLocked ?? false),
                            true,
                            false,
                            0,
                            "calculated");
                    });

                return new GridPeriodCellDto(measures);
            });
    }

    private static decimal DeriveAsp(IReadOnlyList<GridRowDto> rows, long timePeriodId)
    {
        var revenue = rows.Sum(row => row.Cells.GetValueOrDefault(timePeriodId)?.Measures.GetValueOrDefault(PlanningMeasures.SalesRevenue)?.Value ?? 0m);
        var quantity = rows.Sum(row => row.Cells.GetValueOrDefault(timePeriodId)?.Measures.GetValueOrDefault(PlanningMeasures.SoldQuantity)?.Value ?? 0m);
        return quantity <= 0 ? 0m : Math.Round(revenue / quantity, 2, MidpointRounding.AwayFromZero);
    }

    private static decimal DeriveUnitCost(IReadOnlyList<GridRowDto> rows, long timePeriodId)
    {
        var totalCosts = rows.Sum(row => row.Cells.GetValueOrDefault(timePeriodId)?.Measures.GetValueOrDefault(PlanningMeasures.TotalCosts)?.Value ?? 0m);
        var quantity = rows.Sum(row => row.Cells.GetValueOrDefault(timePeriodId)?.Measures.GetValueOrDefault(PlanningMeasures.SoldQuantity)?.Value ?? 0m);
        return quantity <= 0 ? 0m : Math.Round(totalCosts / quantity, 2, MidpointRounding.AwayFromZero);
    }

    private static decimal DeriveGrossProfitPercent(IReadOnlyList<GridRowDto> rows, long timePeriodId)
    {
        var asp = DeriveAsp(rows, timePeriodId);
        var unitCost = DeriveUnitCost(rows, timePeriodId);
        return asp <= 0m ? 0m : Math.Round(((asp - unitCost) / asp) * 100m, 1, MidpointRounding.AwayFromZero);
    }

    private static (long StoreId, long ProductNodeId) ParseCanonicalNodeViewRowId(string viewRowId)
    {
        var segments = viewRowId.Split(':', StringSplitOptions.RemoveEmptyEntries);
        var nodeIndex = Array.FindIndex(segments, segment => string.Equals(segment, "node", StringComparison.OrdinalIgnoreCase));
        if (nodeIndex < 0 || segments.Length <= nodeIndex + 2)
        {
            throw new InvalidOperationException($"Unable to parse planning row id '{viewRowId}'.");
        }

        return (
            long.Parse(segments[nodeIndex + 1], CultureInfo.InvariantCulture),
            long.Parse(segments[nodeIndex + 2], CultureInfo.InvariantCulture));
    }

    private static string[] ParsePathFromViewRowId(string viewRowId)
    {
        return viewRowId switch
        {
            StoreRootViewRowId => ["Store Total"],
            DepartmentRootViewRowId => ["Department Total"],
            _ when viewRowId.StartsWith("view:department:department:", StringComparison.OrdinalIgnoreCase) =>
                ["Department Total", Uri.UnescapeDataString(viewRowId["view:department:department:".Length..])],
            _ when viewRowId.StartsWith("view:department:class:", StringComparison.OrdinalIgnoreCase) =>
                BuildDepartmentClassPath(viewRowId),
            _ when viewRowId.Contains(":node:", StringComparison.OrdinalIgnoreCase) =>
                Uri.UnescapeDataString(viewRowId[(viewRowId.LastIndexOf(':') + 1)..]).Split('>', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            _ => throw new InvalidOperationException($"Unable to derive row path from '{viewRowId}'.")
        };
    }

    private static string[] BuildDepartmentClassPath(string viewRowId)
    {
        var payload = viewRowId["view:department:class:".Length..].Split(':', 2);
        return ["Department Total", Uri.UnescapeDataString(payload[0]), Uri.UnescapeDataString(payload[1])];
    }
}
