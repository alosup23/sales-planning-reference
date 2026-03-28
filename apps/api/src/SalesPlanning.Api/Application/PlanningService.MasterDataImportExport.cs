using System.Globalization;
using ClosedXML.Excel;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    private static readonly string[] InventoryProfileImportHeaders =
    [
        "CompCode", "Product Code", "Starting Inventory", "Inbound Qty", "Reserved Qty",
        "Projected Stock On Hand", "Safety Stock", "Weeks Of Cover Target", "Sell Through Target Pct", "Status"
    ];

    private static readonly string[] PricingPolicyImportHeaders =
    [
        "Department", "Class", "Subclass", "Brand", "Price Ladder Group", "Min Price", "Max Price",
        "Markdown Floor Price", "Minimum Margin Pct", "KVI Flag", "Markdown Eligible", "Status"
    ];

    private static readonly string[] SeasonalityEventImportHeaders =
    [
        "Department", "Class", "Subclass", "Season Code", "Event Code", "Month", "Weight", "Promo Window", "Peak Flag", "Status"
    ];

    private static readonly string[] VendorSupplyImportHeaders =
    [
        "Supplier", "Brand", "Lead Time Days", "MOQ", "Case Pack", "Replenishment Type", "Payment Terms", "Status"
    ];

    public async Task<InventoryProfileImportResponse> ImportInventoryProfilesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            ValidateExcelFileName(fileName, "Inventory Profile");
            using var workbook = new XLWorkbook(workbookStream);
            var sheet = workbook.Worksheet(1);
            var headerMap = GetHeaderMap(sheet);
            ValidateHeaders(headerMap, InventoryProfileImportHeaders, "inventory profile");

            var existingProfiles = (await LoadAllInventoryProfilesAsync(ct))
                .ToDictionary(profile => BuildInventoryProfileKey(profile.StoreCode, profile.ProductCode), StringComparer.OrdinalIgnoreCase);

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet.Name);
            WriteImportHeaderRow(exceptionSheet, InventoryProfileImportHeaders, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var recordsAdded = 0;
            var recordsUpdated = 0;

            foreach (var row in sheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadInventoryProfileImportRow(row, headerMap);
                if (!TryNormalizeInventoryProfileImportRow(importRow, out var normalized, out var error))
                {
                    WriteInventoryProfileExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                var key = BuildInventoryProfileKey(normalized.StoreCode, normalized.ProductCode);
                var existing = existingProfiles.GetValueOrDefault(key);
                var upserted = await _repository.UpsertInventoryProfileAsync(existing is null ? normalized : normalized with { InventoryProfileId = existing.InventoryProfileId }, ct);
                existingProfiles[key] = upserted;
                if (existing is null)
                {
                    recordsAdded += 1;
                }
                else
                {
                    recordsUpdated += 1;
                }
            }

            return BuildInventoryImportResponse(fileName, exceptionWorkbook, exceptionRowIndex, rowsProcessed, recordsAdded, recordsUpdated);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportInventoryProfilesAsync(CancellationToken cancellationToken)
    {
        var profiles = await LoadAllInventoryProfilesAsync(cancellationToken);
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Inventory Profile");
        WriteImportHeaderRow(sheet, InventoryProfileImportHeaders);

        var rowIndex = 2;
        foreach (var profile in profiles.OrderBy(item => item.StoreCode, StringComparer.OrdinalIgnoreCase).ThenBy(item => item.ProductCode, StringComparer.OrdinalIgnoreCase))
        {
            WriteInventoryProfileRow(sheet, rowIndex++, ToInventoryProfileDto(profile));
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "inventory-profile-export.xlsx");
    }

    public async Task<PricingPolicyImportResponse> ImportPricingPoliciesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            ValidateExcelFileName(fileName, "Pricing Policy");
            using var workbook = new XLWorkbook(workbookStream);
            var sheet = workbook.Worksheet(1);
            var headerMap = GetHeaderMap(sheet);
            ValidateHeaders(headerMap, PricingPolicyImportHeaders, "pricing policy");

            var existingPolicies = (await LoadAllPricingPoliciesAsync(ct))
                .ToDictionary(policy => BuildPricingPolicyKey(policy.Department, policy.ClassLabel, policy.Subclass, policy.Brand, policy.PriceLadderGroup), StringComparer.OrdinalIgnoreCase);

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet.Name);
            WriteImportHeaderRow(exceptionSheet, PricingPolicyImportHeaders, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var recordsAdded = 0;
            var recordsUpdated = 0;

            foreach (var row in sheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadPricingPolicyImportRow(row, headerMap);
                if (!TryNormalizePricingPolicyImportRow(importRow, out var normalized, out var error))
                {
                    WritePricingPolicyExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                var key = BuildPricingPolicyKey(normalized.Department, normalized.ClassLabel, normalized.Subclass, normalized.Brand, normalized.PriceLadderGroup);
                var existing = existingPolicies.GetValueOrDefault(key);
                var upserted = await _repository.UpsertPricingPolicyAsync(existing is null ? normalized : normalized with { PricingPolicyId = existing.PricingPolicyId }, ct);
                existingPolicies[key] = upserted;
                if (existing is null)
                {
                    recordsAdded += 1;
                }
                else
                {
                    recordsUpdated += 1;
                }
            }

            return BuildPricingPolicyImportResponse(fileName, exceptionWorkbook, exceptionRowIndex, rowsProcessed, recordsAdded, recordsUpdated);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportPricingPoliciesAsync(CancellationToken cancellationToken)
    {
        var policies = await LoadAllPricingPoliciesAsync(cancellationToken);
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Pricing Policy");
        WriteImportHeaderRow(sheet, PricingPolicyImportHeaders);

        var rowIndex = 2;
        foreach (var policy in policies
                     .OrderBy(item => item.Department ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.ClassLabel ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Subclass ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Brand ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            WritePricingPolicyRow(sheet, rowIndex++, ToPricingPolicyDto(policy));
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "pricing-policy-export.xlsx");
    }

    public async Task<SeasonalityEventProfileImportResponse> ImportSeasonalityEventProfilesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            ValidateExcelFileName(fileName, "Seasonality & Events");
            using var workbook = new XLWorkbook(workbookStream);
            var sheet = workbook.Worksheet(1);
            var headerMap = GetHeaderMap(sheet);
            ValidateHeaders(headerMap, SeasonalityEventImportHeaders, "seasonality & events");

            var existingProfiles = (await LoadAllSeasonalityEventProfilesAsync(ct))
                .ToDictionary(profile => BuildSeasonalityEventKey(profile.Department, profile.ClassLabel, profile.Subclass, profile.SeasonCode, profile.EventCode, profile.Month), StringComparer.OrdinalIgnoreCase);

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet.Name);
            WriteImportHeaderRow(exceptionSheet, SeasonalityEventImportHeaders, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var recordsAdded = 0;
            var recordsUpdated = 0;

            foreach (var row in sheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadSeasonalityEventImportRow(row, headerMap);
                if (!TryNormalizeSeasonalityEventImportRow(importRow, out var normalized, out var error))
                {
                    WriteSeasonalityEventExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                var key = BuildSeasonalityEventKey(normalized.Department, normalized.ClassLabel, normalized.Subclass, normalized.SeasonCode, normalized.EventCode, normalized.Month);
                var existing = existingProfiles.GetValueOrDefault(key);
                var upserted = await _repository.UpsertSeasonalityEventProfileAsync(existing is null ? normalized : normalized with { SeasonalityEventProfileId = existing.SeasonalityEventProfileId }, ct);
                existingProfiles[key] = upserted;
                if (existing is null)
                {
                    recordsAdded += 1;
                }
                else
                {
                    recordsUpdated += 1;
                }
            }

            return BuildSeasonalityImportResponse(fileName, exceptionWorkbook, exceptionRowIndex, rowsProcessed, recordsAdded, recordsUpdated);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportSeasonalityEventProfilesAsync(CancellationToken cancellationToken)
    {
        var profiles = await LoadAllSeasonalityEventProfilesAsync(cancellationToken);
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Seasonality & Events");
        WriteImportHeaderRow(sheet, SeasonalityEventImportHeaders);

        var rowIndex = 2;
        foreach (var profile in profiles
                     .OrderBy(item => item.Month)
                     .ThenBy(item => item.Department ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.ClassLabel ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Subclass ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            WriteSeasonalityEventRow(sheet, rowIndex++, ToSeasonalityEventProfileDto(profile));
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "seasonality-events-export.xlsx");
    }

    public async Task<VendorSupplyProfileImportResponse> ImportVendorSupplyProfilesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            ValidateExcelFileName(fileName, "Vendor Supply Profile");
            using var workbook = new XLWorkbook(workbookStream);
            var sheet = workbook.Worksheet(1);
            var headerMap = GetHeaderMap(sheet);
            ValidateHeaders(headerMap, VendorSupplyImportHeaders, "vendor supply profile");

            var existingProfiles = (await LoadAllVendorSupplyProfilesAsync(ct))
                .ToDictionary(profile => BuildVendorSupplyKey(profile.Supplier, profile.Brand), StringComparer.OrdinalIgnoreCase);

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet.Name);
            WriteImportHeaderRow(exceptionSheet, VendorSupplyImportHeaders, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var recordsAdded = 0;
            var recordsUpdated = 0;

            foreach (var row in sheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadVendorSupplyImportRow(row, headerMap);
                if (!TryNormalizeVendorSupplyImportRow(importRow, out var normalized, out var error))
                {
                    WriteVendorSupplyExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                var key = BuildVendorSupplyKey(normalized.Supplier, normalized.Brand);
                var existing = existingProfiles.GetValueOrDefault(key);
                var upserted = await _repository.UpsertVendorSupplyProfileAsync(existing is null ? normalized : normalized with { VendorSupplyProfileId = existing.VendorSupplyProfileId }, ct);
                existingProfiles[key] = upserted;
                if (existing is null)
                {
                    recordsAdded += 1;
                }
                else
                {
                    recordsUpdated += 1;
                }
            }

            return BuildVendorSupplyImportResponse(fileName, exceptionWorkbook, exceptionRowIndex, rowsProcessed, recordsAdded, recordsUpdated);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportVendorSupplyProfilesAsync(CancellationToken cancellationToken)
    {
        var profiles = await LoadAllVendorSupplyProfilesAsync(cancellationToken);
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Vendor Supply Profile");
        WriteImportHeaderRow(sheet, VendorSupplyImportHeaders);

        var rowIndex = 2;
        foreach (var profile in profiles.OrderBy(item => item.Supplier, StringComparer.OrdinalIgnoreCase).ThenBy(item => item.Brand ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            WriteVendorSupplyRow(sheet, rowIndex++, ToVendorSupplyProfileDto(profile));
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "vendor-supply-profile-export.xlsx");
    }

    private async Task<IReadOnlyList<InventoryProfileRecord>> LoadAllInventoryProfilesAsync(CancellationToken cancellationToken)
    {
        const int pageSize = 500;
        var page = 1;
        var items = new List<InventoryProfileRecord>();
        while (true)
        {
            var (profiles, totalCount) = await _repository.GetInventoryProfilesAsync(null, page, pageSize, cancellationToken);
            items.AddRange(profiles);
            if (items.Count >= totalCount || profiles.Count == 0)
            {
                break;
            }

            page += 1;
        }

        return items;
    }

    private async Task<IReadOnlyList<PricingPolicyRecord>> LoadAllPricingPoliciesAsync(CancellationToken cancellationToken)
    {
        const int pageSize = 500;
        var page = 1;
        var items = new List<PricingPolicyRecord>();
        while (true)
        {
            var (policies, totalCount) = await _repository.GetPricingPoliciesAsync(null, page, pageSize, cancellationToken);
            items.AddRange(policies);
            if (items.Count >= totalCount || policies.Count == 0)
            {
                break;
            }

            page += 1;
        }

        return items;
    }

    private async Task<IReadOnlyList<SeasonalityEventProfileRecord>> LoadAllSeasonalityEventProfilesAsync(CancellationToken cancellationToken)
    {
        const int pageSize = 500;
        var page = 1;
        var items = new List<SeasonalityEventProfileRecord>();
        while (true)
        {
            var (profiles, totalCount) = await _repository.GetSeasonalityEventProfilesAsync(null, page, pageSize, cancellationToken);
            items.AddRange(profiles);
            if (items.Count >= totalCount || profiles.Count == 0)
            {
                break;
            }

            page += 1;
        }

        return items;
    }

    private async Task<IReadOnlyList<VendorSupplyProfileRecord>> LoadAllVendorSupplyProfilesAsync(CancellationToken cancellationToken)
    {
        const int pageSize = 500;
        var page = 1;
        var items = new List<VendorSupplyProfileRecord>();
        while (true)
        {
            var (profiles, totalCount) = await _repository.GetVendorSupplyProfilesAsync(null, page, pageSize, cancellationToken);
            items.AddRange(profiles);
            if (items.Count >= totalCount || profiles.Count == 0)
            {
                break;
            }

            page += 1;
        }

        return items;
    }

    private static void ValidateExcelFileName(string fileName, string context)
    {
        if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Only .xlsx workbook uploads are supported for {context} maintenance.");
        }
    }

    private static void ValidateHeaders(IReadOnlyDictionary<string, int> headerMap, IEnumerable<string> requiredHeaders, string workbookLabel)
    {
        foreach (var header in requiredHeaders)
        {
            if (!headerMap.ContainsKey(header))
            {
                throw new InvalidOperationException($"The {workbookLabel} workbook is missing the required '{header}' column.");
            }
        }
    }

    private static void WriteImportHeaderRow(IXLWorksheet sheet, IReadOnlyList<string> headers, bool includeRemark = false)
    {
        var values = includeRemark ? [.. headers, RemarkHeader] : headers;
        for (var index = 0; index < values.Count; index += 1)
        {
            sheet.Cell(1, index + 1).Value = values[index];
        }
    }

    private static ImportedInventoryProfileRow ReadInventoryProfileImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedInventoryProfileRow(
            GetValue("CompCode"),
            GetValue("Product Code"),
            GetValue("Starting Inventory"),
            GetValue("Inbound Qty"),
            GetValue("Reserved Qty"),
            GetValue("Projected Stock On Hand"),
            GetValue("Safety Stock"),
            GetValue("Weeks Of Cover Target"),
            GetValue("Sell Through Target Pct"),
            GetValue("Status"),
            GetValue(RemarkHeader));
    }

    private static bool TryNormalizeInventoryProfileImportRow(ImportedInventoryProfileRow row, out InventoryProfileRecord normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        var storeCode = NormalizeOptionalText(row.CompCode);
        var productCode = NormalizeOptionalText(row.ProductCode);
        if (storeCode is null || productCode is null)
        {
            error = "CompCode and Product Code are required.";
            return false;
        }

        if (!TryParseRequiredDecimal(row.StartingInventory, out var startingInventory))
        {
            error = "Starting Inventory must be numeric.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.InboundQty, out var inboundQty) ||
            !TryParseOptionalDecimal(row.ReservedQty, out var reservedQty) ||
            !TryParseOptionalDecimal(row.ProjectedStockOnHand, out var projectedStockOnHand) ||
            !TryParseOptionalDecimal(row.SafetyStock, out var safetyStock) ||
            !TryParseOptionalDecimal(row.WeeksOfCoverTarget, out var weeksOfCoverTarget) ||
            !TryParseOptionalDecimal(row.SellThroughTargetPct, out var sellThroughTargetPct))
        {
            error = "Inventory quantity and target fields must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.Status, out var isActive))
        {
            error = "Status must be Active/Inactive or true/false when provided.";
            return false;
        }

        normalized = new InventoryProfileRecord(
            0,
            storeCode,
            productCode,
            startingInventory,
            inboundQty,
            reservedQty,
            projectedStockOnHand,
            safetyStock,
            weeksOfCoverTarget,
            sellThroughTargetPct,
            isActive ?? true);
        return true;
    }

    private static ImportedPricingPolicyRow ReadPricingPolicyImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedPricingPolicyRow(
            GetValue("Department"),
            GetValue("Class"),
            GetValue("Subclass"),
            GetValue("Brand"),
            GetValue("Price Ladder Group"),
            GetValue("Min Price"),
            GetValue("Max Price"),
            GetValue("Markdown Floor Price"),
            GetValue("Minimum Margin Pct"),
            GetValue("KVI Flag"),
            GetValue("Markdown Eligible"),
            GetValue("Status"),
            GetValue(RemarkHeader));
    }

    private static bool TryNormalizePricingPolicyImportRow(ImportedPricingPolicyRow row, out PricingPolicyRecord normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        var department = NormalizeOptionalText(row.Department);
        var classLabel = NormalizeOptionalText(row.Class);
        var subclass = NormalizeOptionalText(row.Subclass);
        var brand = NormalizeOptionalText(row.Brand);
        var priceLadderGroup = NormalizeOptionalText(row.PriceLadderGroup);
        if (department is null && classLabel is null && subclass is null && brand is null && priceLadderGroup is null)
        {
            error = "At least one pricing scope field is required.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.MinPrice, out var minPrice) ||
            !TryParseOptionalDecimal(row.MaxPrice, out var maxPrice) ||
            !TryParseOptionalDecimal(row.MarkdownFloorPrice, out var markdownFloorPrice) ||
            !TryParseOptionalDecimal(row.MinimumMarginPct, out var minimumMarginPct))
        {
            error = "Price and margin fields must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.KviFlag, out var kviFlag) ||
            !TryParseOptionalBoolean(row.MarkdownEligible, out var markdownEligible) ||
            !TryParseOptionalBoolean(row.Status, out var isActive))
        {
            error = "KVI Flag, Markdown Eligible, and Status must be Active/Inactive or true/false when provided.";
            return false;
        }

        normalized = new PricingPolicyRecord(
            0,
            department,
            classLabel,
            subclass,
            brand,
            priceLadderGroup,
            minPrice,
            maxPrice,
            markdownFloorPrice,
            minimumMarginPct,
            kviFlag ?? false,
            markdownEligible ?? true,
            isActive ?? true);
        return true;
    }

    private static ImportedSeasonalityEventRow ReadSeasonalityEventImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedSeasonalityEventRow(
            GetValue("Department"),
            GetValue("Class"),
            GetValue("Subclass"),
            GetValue("Season Code"),
            GetValue("Event Code"),
            GetValue("Month"),
            GetValue("Weight"),
            GetValue("Promo Window"),
            GetValue("Peak Flag"),
            GetValue("Status"),
            GetValue(RemarkHeader));
    }

    private static bool TryNormalizeSeasonalityEventImportRow(ImportedSeasonalityEventRow row, out SeasonalityEventProfileRecord normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        var department = NormalizeOptionalText(row.Department);
        var classLabel = NormalizeOptionalText(row.Class);
        var subclass = NormalizeOptionalText(row.Subclass);
        var seasonCode = NormalizeOptionalText(row.SeasonCode);
        var eventCode = NormalizeOptionalText(row.EventCode);
        if (department is null && classLabel is null && subclass is null && seasonCode is null && eventCode is null)
        {
            error = "At least one seasonality scope field is required.";
            return false;
        }

        if (!int.TryParse(row.Month, NumberStyles.Integer, CultureInfo.InvariantCulture, out var month) || month is < 1 or > 12)
        {
            error = "Month must be an integer between 1 and 12.";
            return false;
        }

        if (!TryParseRequiredDecimal(row.Weight, out var weight))
        {
            error = "Weight must be numeric.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.PeakFlag, out var peakFlag) ||
            !TryParseOptionalBoolean(row.Status, out var isActive))
        {
            error = "Peak Flag and Status must be Active/Inactive or true/false when provided.";
            return false;
        }

        normalized = new SeasonalityEventProfileRecord(
            0,
            department,
            classLabel,
            subclass,
            seasonCode,
            eventCode,
            month,
            weight,
            NormalizeOptionalText(row.PromoWindow),
            peakFlag ?? false,
            isActive ?? true);
        return true;
    }

    private static ImportedVendorSupplyRow ReadVendorSupplyImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedVendorSupplyRow(
            GetValue("Supplier"),
            GetValue("Brand"),
            GetValue("Lead Time Days"),
            GetValue("MOQ"),
            GetValue("Case Pack"),
            GetValue("Replenishment Type"),
            GetValue("Payment Terms"),
            GetValue("Status"),
            GetValue(RemarkHeader));
    }

    private static bool TryNormalizeVendorSupplyImportRow(ImportedVendorSupplyRow row, out VendorSupplyProfileRecord normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        var supplier = NormalizeOptionalText(row.Supplier);
        if (supplier is null)
        {
            error = "Supplier is required.";
            return false;
        }

        if (!TryParseOptionalInt(row.LeadTimeDays, out var leadTimeDays) ||
            !TryParseOptionalInt(row.Moq, out var moq) ||
            !TryParseOptionalInt(row.CasePack, out var casePack))
        {
            error = "Lead Time Days, MOQ, and Case Pack must be integers when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.Status, out var isActive))
        {
            error = "Status must be Active/Inactive or true/false when provided.";
            return false;
        }

        normalized = new VendorSupplyProfileRecord(
            0,
            supplier,
            NormalizeOptionalText(row.Brand),
            leadTimeDays,
            moq,
            casePack,
            NormalizeOptionalText(row.ReplenishmentType),
            NormalizeOptionalText(row.PaymentTerms),
            isActive ?? true);
        return true;
    }

    private static void WriteInventoryProfileRow(IXLWorksheet sheet, int rowIndex, InventoryProfileDto profile)
    {
        sheet.Cell(rowIndex, 1).Value = profile.StoreCode;
        sheet.Cell(rowIndex, 2).Value = profile.ProductCode;
        sheet.Cell(rowIndex, 3).Value = profile.StartingInventory;
        sheet.Cell(rowIndex, 4).Value = profile.InboundQty;
        sheet.Cell(rowIndex, 5).Value = profile.ReservedQty;
        sheet.Cell(rowIndex, 6).Value = profile.ProjectedStockOnHand;
        sheet.Cell(rowIndex, 7).Value = profile.SafetyStock;
        sheet.Cell(rowIndex, 8).Value = profile.WeeksOfCoverTarget;
        sheet.Cell(rowIndex, 9).Value = profile.SellThroughTargetPct;
        sheet.Cell(rowIndex, 10).Value = profile.IsActive ? "Active" : "Inactive";
    }

    private static void WritePricingPolicyRow(IXLWorksheet sheet, int rowIndex, PricingPolicyDto policy)
    {
        sheet.Cell(rowIndex, 1).Value = policy.Department;
        sheet.Cell(rowIndex, 2).Value = policy.Class;
        sheet.Cell(rowIndex, 3).Value = policy.Subclass;
        sheet.Cell(rowIndex, 4).Value = policy.Brand;
        sheet.Cell(rowIndex, 5).Value = policy.PriceLadderGroup;
        sheet.Cell(rowIndex, 6).Value = policy.MinPrice;
        sheet.Cell(rowIndex, 7).Value = policy.MaxPrice;
        sheet.Cell(rowIndex, 8).Value = policy.MarkdownFloorPrice;
        sheet.Cell(rowIndex, 9).Value = policy.MinimumMarginPct;
        sheet.Cell(rowIndex, 10).Value = policy.KviFlag ? "true" : "false";
        sheet.Cell(rowIndex, 11).Value = policy.MarkdownEligible ? "true" : "false";
        sheet.Cell(rowIndex, 12).Value = policy.IsActive ? "Active" : "Inactive";
    }

    private static void WriteSeasonalityEventRow(IXLWorksheet sheet, int rowIndex, SeasonalityEventProfileDto profile)
    {
        sheet.Cell(rowIndex, 1).Value = profile.Department;
        sheet.Cell(rowIndex, 2).Value = profile.Class;
        sheet.Cell(rowIndex, 3).Value = profile.Subclass;
        sheet.Cell(rowIndex, 4).Value = profile.SeasonCode;
        sheet.Cell(rowIndex, 5).Value = profile.EventCode;
        sheet.Cell(rowIndex, 6).Value = profile.Month;
        sheet.Cell(rowIndex, 7).Value = profile.Weight;
        sheet.Cell(rowIndex, 8).Value = profile.PromoWindow;
        sheet.Cell(rowIndex, 9).Value = profile.PeakFlag ? "true" : "false";
        sheet.Cell(rowIndex, 10).Value = profile.IsActive ? "Active" : "Inactive";
    }

    private static void WriteVendorSupplyRow(IXLWorksheet sheet, int rowIndex, VendorSupplyProfileDto profile)
    {
        sheet.Cell(rowIndex, 1).Value = profile.Supplier;
        sheet.Cell(rowIndex, 2).Value = profile.Brand;
        sheet.Cell(rowIndex, 3).Value = profile.LeadTimeDays;
        sheet.Cell(rowIndex, 4).Value = profile.Moq;
        sheet.Cell(rowIndex, 5).Value = profile.CasePack;
        sheet.Cell(rowIndex, 6).Value = profile.ReplenishmentType;
        sheet.Cell(rowIndex, 7).Value = profile.PaymentTerms;
        sheet.Cell(rowIndex, 8).Value = profile.IsActive ? "Active" : "Inactive";
    }

    private static void WriteInventoryProfileExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedInventoryProfileRow row, string error)
    {
        WriteExceptionRow(sheet, rowIndex,
        [
            row.CompCode, row.ProductCode, row.StartingInventory, row.InboundQty, row.ReservedQty, row.ProjectedStockOnHand,
            row.SafetyStock, row.WeeksOfCoverTarget, row.SellThroughTargetPct, row.Status, error
        ]);
    }

    private static void WritePricingPolicyExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedPricingPolicyRow row, string error)
    {
        WriteExceptionRow(sheet, rowIndex,
        [
            row.Department, row.Class, row.Subclass, row.Brand, row.PriceLadderGroup, row.MinPrice, row.MaxPrice,
            row.MarkdownFloorPrice, row.MinimumMarginPct, row.KviFlag, row.MarkdownEligible, row.Status, error
        ]);
    }

    private static void WriteSeasonalityEventExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedSeasonalityEventRow row, string error)
    {
        WriteExceptionRow(sheet, rowIndex,
        [
            row.Department, row.Class, row.Subclass, row.SeasonCode, row.EventCode, row.Month, row.Weight, row.PromoWindow,
            row.PeakFlag, row.Status, error
        ]);
    }

    private static void WriteVendorSupplyExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedVendorSupplyRow row, string error)
    {
        WriteExceptionRow(sheet, rowIndex,
        [
            row.Supplier, row.Brand, row.LeadTimeDays, row.Moq, row.CasePack, row.ReplenishmentType, row.PaymentTerms, row.Status, error
        ]);
    }

    private static void WriteExceptionRow(IXLWorksheet sheet, int rowIndex, IReadOnlyList<string> values)
    {
        for (var index = 0; index < values.Count; index += 1)
        {
            sheet.Cell(rowIndex, index + 1).Value = values[index];
        }

        sheet.Row(rowIndex).Style.Fill.BackgroundColor = XLColor.LightPink;
    }

    private static InventoryProfileImportResponse BuildInventoryImportResponse(string fileName, XLWorkbook exceptionWorkbook, int exceptionRowIndex, int rowsProcessed, int recordsAdded, int recordsUpdated)
    {
        BuildExceptionWorkbook(exceptionWorkbook, exceptionRowIndex, fileName, out var exceptionFileName, out var exceptionWorkbookBase64);
        return new InventoryProfileImportResponse(rowsProcessed, recordsAdded, recordsUpdated, "applied", exceptionFileName, exceptionWorkbookBase64);
    }

    private static PricingPolicyImportResponse BuildPricingPolicyImportResponse(string fileName, XLWorkbook exceptionWorkbook, int exceptionRowIndex, int rowsProcessed, int recordsAdded, int recordsUpdated)
    {
        BuildExceptionWorkbook(exceptionWorkbook, exceptionRowIndex, fileName, out var exceptionFileName, out var exceptionWorkbookBase64);
        return new PricingPolicyImportResponse(rowsProcessed, recordsAdded, recordsUpdated, "applied", exceptionFileName, exceptionWorkbookBase64);
    }

    private static SeasonalityEventProfileImportResponse BuildSeasonalityImportResponse(string fileName, XLWorkbook exceptionWorkbook, int exceptionRowIndex, int rowsProcessed, int recordsAdded, int recordsUpdated)
    {
        BuildExceptionWorkbook(exceptionWorkbook, exceptionRowIndex, fileName, out var exceptionFileName, out var exceptionWorkbookBase64);
        return new SeasonalityEventProfileImportResponse(rowsProcessed, recordsAdded, recordsUpdated, "applied", exceptionFileName, exceptionWorkbookBase64);
    }

    private static VendorSupplyProfileImportResponse BuildVendorSupplyImportResponse(string fileName, XLWorkbook exceptionWorkbook, int exceptionRowIndex, int rowsProcessed, int recordsAdded, int recordsUpdated)
    {
        BuildExceptionWorkbook(exceptionWorkbook, exceptionRowIndex, fileName, out var exceptionFileName, out var exceptionWorkbookBase64);
        return new VendorSupplyProfileImportResponse(rowsProcessed, recordsAdded, recordsUpdated, "applied", exceptionFileName, exceptionWorkbookBase64);
    }

    private static void BuildExceptionWorkbook(XLWorkbook exceptionWorkbook, int exceptionRowIndex, string fileName, out string? exceptionFileName, out string? exceptionWorkbookBase64)
    {
        exceptionWorkbookBase64 = null;
        exceptionFileName = null;
        if (exceptionRowIndex <= 2)
        {
            return;
        }

        using var exceptionStream = new MemoryStream();
        exceptionWorkbook.SaveAs(exceptionStream);
        exceptionWorkbookBase64 = Convert.ToBase64String(exceptionStream.ToArray());
        exceptionFileName = $"{Path.GetFileNameWithoutExtension(fileName)}-exceptions.xlsx";
    }

    private static string BuildInventoryProfileKey(string storeCode, string productCode) =>
        $"{storeCode.Trim().ToUpperInvariant()}|{productCode.Trim().ToUpperInvariant()}";

    private static string BuildPricingPolicyKey(string? department, string? classLabel, string? subclass, string? brand, string? priceLadderGroup) =>
        $"{(department ?? string.Empty).Trim().ToUpperInvariant()}|{(classLabel ?? string.Empty).Trim().ToUpperInvariant()}|{(subclass ?? string.Empty).Trim().ToUpperInvariant()}|{(brand ?? string.Empty).Trim().ToUpperInvariant()}|{(priceLadderGroup ?? string.Empty).Trim().ToUpperInvariant()}";

    private static string BuildSeasonalityEventKey(string? department, string? classLabel, string? subclass, string? seasonCode, string? eventCode, int month) =>
        $"{(department ?? string.Empty).Trim().ToUpperInvariant()}|{(classLabel ?? string.Empty).Trim().ToUpperInvariant()}|{(subclass ?? string.Empty).Trim().ToUpperInvariant()}|{(seasonCode ?? string.Empty).Trim().ToUpperInvariant()}|{(eventCode ?? string.Empty).Trim().ToUpperInvariant()}|{month}";

    private static string BuildVendorSupplyKey(string supplier, string? brand) =>
        $"{supplier.Trim().ToUpperInvariant()}|{(brand ?? string.Empty).Trim().ToUpperInvariant()}";

    private readonly record struct ImportedInventoryProfileRow(
        string CompCode,
        string ProductCode,
        string StartingInventory,
        string InboundQty,
        string ReservedQty,
        string ProjectedStockOnHand,
        string SafetyStock,
        string WeeksOfCoverTarget,
        string SellThroughTargetPct,
        string Status,
        string Remark);

    private readonly record struct ImportedPricingPolicyRow(
        string Department,
        string Class,
        string Subclass,
        string Brand,
        string PriceLadderGroup,
        string MinPrice,
        string MaxPrice,
        string MarkdownFloorPrice,
        string MinimumMarginPct,
        string KviFlag,
        string MarkdownEligible,
        string Status,
        string Remark);

    private readonly record struct ImportedSeasonalityEventRow(
        string Department,
        string Class,
        string Subclass,
        string SeasonCode,
        string EventCode,
        string Month,
        string Weight,
        string PromoWindow,
        string PeakFlag,
        string Status,
        string Remark);

    private readonly record struct ImportedVendorSupplyRow(
        string Supplier,
        string Brand,
        string LeadTimeDays,
        string Moq,
        string CasePack,
        string ReplenishmentType,
        string PaymentTerms,
        string Status,
        string Remark);
}
