using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    public async Task<InventoryProfileResponse> GetInventoryProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        var (profiles, totalCount) = await _repository.GetInventoryProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
        return new InventoryProfileResponse(
            profiles.Select(ToInventoryProfileDto).ToList(),
            totalCount,
            Math.Max(1, pageNumber),
            Math.Clamp(pageSize, 25, 500),
            string.IsNullOrWhiteSpace(searchTerm) ? null : searchTerm.Trim());
    }

    public async Task<InventoryProfileDto> UpsertInventoryProfileAsync(UpsertInventoryProfileRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertInventoryProfileAsync(NormalizeInventoryProfile(request), cancellationToken);
        return ToInventoryProfileDto(upserted);
    }

    public Task DeleteInventoryProfileAsync(DeleteInventoryProfileRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeleteInventoryProfileAsync(request.InventoryProfileId, cancellationToken);
    }

    public async Task<InventoryProfileDto> InactivateInventoryProfileAsync(InactivateInventoryProfileRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivateInventoryProfileAsync(request.InventoryProfileId, cancellationToken);
        return ToInventoryProfileDto(await _repository.GetInventoryProfileByIdAsync(request.InventoryProfileId, cancellationToken));
    }

    public async Task<PricingPolicyResponse> GetPricingPoliciesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        var (policies, totalCount) = await _repository.GetPricingPoliciesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
        return new PricingPolicyResponse(
            policies.Select(ToPricingPolicyDto).ToList(),
            totalCount,
            Math.Max(1, pageNumber),
            Math.Clamp(pageSize, 25, 500),
            string.IsNullOrWhiteSpace(searchTerm) ? null : searchTerm.Trim());
    }

    public async Task<PricingPolicyDto> UpsertPricingPolicyAsync(UpsertPricingPolicyRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertPricingPolicyAsync(NormalizePricingPolicy(request), cancellationToken);
        return ToPricingPolicyDto(upserted);
    }

    public Task DeletePricingPolicyAsync(DeletePricingPolicyRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeletePricingPolicyAsync(request.PricingPolicyId, cancellationToken);
    }

    public async Task<PricingPolicyDto> InactivatePricingPolicyAsync(InactivatePricingPolicyRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivatePricingPolicyAsync(request.PricingPolicyId, cancellationToken);
        return ToPricingPolicyDto(await _repository.GetPricingPolicyByIdAsync(request.PricingPolicyId, cancellationToken));
    }

    public async Task<SeasonalityEventProfileResponse> GetSeasonalityEventProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        var (profiles, totalCount) = await _repository.GetSeasonalityEventProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
        return new SeasonalityEventProfileResponse(
            profiles.Select(ToSeasonalityEventProfileDto).ToList(),
            totalCount,
            Math.Max(1, pageNumber),
            Math.Clamp(pageSize, 25, 500),
            string.IsNullOrWhiteSpace(searchTerm) ? null : searchTerm.Trim());
    }

    public async Task<SeasonalityEventProfileDto> UpsertSeasonalityEventProfileAsync(UpsertSeasonalityEventProfileRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertSeasonalityEventProfileAsync(NormalizeSeasonalityEventProfile(request), cancellationToken);
        return ToSeasonalityEventProfileDto(upserted);
    }

    public Task DeleteSeasonalityEventProfileAsync(DeleteSeasonalityEventProfileRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeleteSeasonalityEventProfileAsync(request.SeasonalityEventProfileId, cancellationToken);
    }

    public async Task<SeasonalityEventProfileDto> InactivateSeasonalityEventProfileAsync(InactivateSeasonalityEventProfileRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivateSeasonalityEventProfileAsync(request.SeasonalityEventProfileId, cancellationToken);
        return ToSeasonalityEventProfileDto(await _repository.GetSeasonalityEventProfileByIdAsync(request.SeasonalityEventProfileId, cancellationToken));
    }

    public async Task<VendorSupplyProfileResponse> GetVendorSupplyProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        var (profiles, totalCount) = await _repository.GetVendorSupplyProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
        return new VendorSupplyProfileResponse(
            profiles.Select(ToVendorSupplyProfileDto).ToList(),
            totalCount,
            Math.Max(1, pageNumber),
            Math.Clamp(pageSize, 25, 500),
            string.IsNullOrWhiteSpace(searchTerm) ? null : searchTerm.Trim());
    }

    public async Task<VendorSupplyProfileDto> UpsertVendorSupplyProfileAsync(UpsertVendorSupplyProfileRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertVendorSupplyProfileAsync(NormalizeVendorSupplyProfile(request), cancellationToken);
        return ToVendorSupplyProfileDto(upserted);
    }

    public Task DeleteVendorSupplyProfileAsync(DeleteVendorSupplyProfileRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeleteVendorSupplyProfileAsync(request.VendorSupplyProfileId, cancellationToken);
    }

    public async Task<VendorSupplyProfileDto> InactivateVendorSupplyProfileAsync(InactivateVendorSupplyProfileRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivateVendorSupplyProfileAsync(request.VendorSupplyProfileId, cancellationToken);
        return ToVendorSupplyProfileDto(await _repository.GetVendorSupplyProfileByIdAsync(request.VendorSupplyProfileId, cancellationToken));
    }

    private static InventoryProfileDto ToInventoryProfileDto(InventoryProfileRecord profile) => new(
        profile.InventoryProfileId,
        profile.StoreCode,
        profile.ProductCode,
        profile.StartingInventory,
        profile.InboundQty,
        profile.ReservedQty,
        profile.ProjectedStockOnHand,
        profile.SafetyStock,
        profile.WeeksOfCoverTarget,
        profile.SellThroughTargetPct,
        profile.IsActive);

    private static InventoryProfileRecord NormalizeInventoryProfile(UpsertInventoryProfileRequest request) => new(
        request.InventoryProfileId ?? 0,
        NormalizeRequiredText(request.StoreCode, null, "Store Code"),
        NormalizeRequiredText(request.ProductCode, null, "Product Code"),
        request.StartingInventory,
        request.InboundQty,
        request.ReservedQty,
        request.ProjectedStockOnHand,
        request.SafetyStock,
        request.WeeksOfCoverTarget,
        request.SellThroughTargetPct,
        request.IsActive);

    private static PricingPolicyDto ToPricingPolicyDto(PricingPolicyRecord policy) => new(
        policy.PricingPolicyId,
        policy.Department,
        policy.ClassLabel,
        policy.Subclass,
        policy.Brand,
        policy.PriceLadderGroup,
        policy.MinPrice,
        policy.MaxPrice,
        policy.MarkdownFloorPrice,
        policy.MinimumMarginPct,
        policy.KviFlag,
        policy.MarkdownEligible,
        policy.IsActive);

    private static PricingPolicyRecord NormalizePricingPolicy(UpsertPricingPolicyRequest request)
    {
        var department = NormalizeOptionalText(request.Department);
        var classLabel = NormalizeOptionalText(request.Class);
        var subclass = NormalizeOptionalText(request.Subclass);
        var brand = NormalizeOptionalText(request.Brand);
        var priceLadderGroup = NormalizeOptionalText(request.PriceLadderGroup);
        if (department is null && classLabel is null && subclass is null && brand is null && priceLadderGroup is null)
        {
            throw new InvalidOperationException("At least one pricing scope field is required.");
        }

        return new PricingPolicyRecord(
            request.PricingPolicyId ?? 0,
            department,
            classLabel,
            subclass,
            brand,
            priceLadderGroup,
            request.MinPrice,
            request.MaxPrice,
            request.MarkdownFloorPrice,
            request.MinimumMarginPct,
            request.KviFlag,
            request.MarkdownEligible,
            request.IsActive);
    }

    private static SeasonalityEventProfileDto ToSeasonalityEventProfileDto(SeasonalityEventProfileRecord profile) => new(
        profile.SeasonalityEventProfileId,
        profile.Department,
        profile.ClassLabel,
        profile.Subclass,
        profile.SeasonCode,
        profile.EventCode,
        profile.Month,
        profile.Weight,
        profile.PromoWindow,
        profile.PeakFlag,
        profile.IsActive);

    private static SeasonalityEventProfileRecord NormalizeSeasonalityEventProfile(UpsertSeasonalityEventProfileRequest request)
    {
        if (request.Month < 1 || request.Month > 12)
        {
            throw new InvalidOperationException("Month must be between 1 and 12.");
        }

        var department = NormalizeOptionalText(request.Department);
        var classLabel = NormalizeOptionalText(request.Class);
        var subclass = NormalizeOptionalText(request.Subclass);
        var seasonCode = NormalizeOptionalText(request.SeasonCode);
        var eventCode = NormalizeOptionalText(request.EventCode);
        if (department is null && classLabel is null && subclass is null && seasonCode is null && eventCode is null)
        {
            throw new InvalidOperationException("At least one seasonality scope field is required.");
        }

        return new SeasonalityEventProfileRecord(
            request.SeasonalityEventProfileId ?? 0,
            department,
            classLabel,
            subclass,
            seasonCode,
            eventCode,
            request.Month,
            request.Weight,
            NormalizeOptionalText(request.PromoWindow),
            request.PeakFlag,
            request.IsActive);
    }

    private static VendorSupplyProfileDto ToVendorSupplyProfileDto(VendorSupplyProfileRecord profile) => new(
        profile.VendorSupplyProfileId,
        profile.Supplier,
        profile.Brand,
        profile.LeadTimeDays,
        profile.Moq,
        profile.CasePack,
        profile.ReplenishmentType,
        profile.PaymentTerms,
        profile.IsActive);

    private static VendorSupplyProfileRecord NormalizeVendorSupplyProfile(UpsertVendorSupplyProfileRequest request) => new(
        request.VendorSupplyProfileId ?? 0,
        NormalizeRequiredText(request.Supplier, null, "Supplier"),
        NormalizeOptionalText(request.Brand),
        request.LeadTimeDays,
        request.Moq,
        request.CasePack,
        NormalizeOptionalText(request.ReplenishmentType),
        NormalizeOptionalText(request.PaymentTerms),
        request.IsActive);
}
