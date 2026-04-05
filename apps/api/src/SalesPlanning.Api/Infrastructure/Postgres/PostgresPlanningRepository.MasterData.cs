using System.Globalization;
using Npgsql;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresPlanningRepository
{
    private async Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> GetInventoryProfilesDirectAsync(
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadInventoryProfilesDirectAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    private async Task<InventoryProfileRecord> GetInventoryProfileByIdDirectAsync(long inventoryProfileId, CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadInventoryProfileByIdDirectAsync(connection, null, inventoryProfileId, cancellationToken);
    }

    private Task<InventoryProfileRecord> UpsertInventoryProfileDirectAsync(InventoryProfileRecord profile, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertInventoryProfileInternalDirectAsync(connection, transaction, profile, ct),
            cancellationToken);

    private Task DeleteInventoryProfileDirectAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("delete from inventory_profiles where inventory_profile_id = @inventoryProfileId;", connection, transaction);
                command.Parameters.AddWithValue("@inventoryProfileId", inventoryProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private Task InactivateInventoryProfileDirectAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand(
                    """
                    update inventory_profiles
                    set is_active = 0
                    where inventory_profile_id = @inventoryProfileId;
                    """,
                    connection,
                    transaction);
                command.Parameters.AddWithValue("@inventoryProfileId", inventoryProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private async Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> GetPricingPoliciesDirectAsync(
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadPricingPoliciesDirectAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    private async Task<PricingPolicyRecord> GetPricingPolicyByIdDirectAsync(long pricingPolicyId, CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadPricingPolicyByIdDirectAsync(connection, null, pricingPolicyId, cancellationToken);
    }

    private Task<PricingPolicyRecord> UpsertPricingPolicyDirectAsync(PricingPolicyRecord policy, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertPricingPolicyInternalDirectAsync(connection, transaction, policy, ct),
            cancellationToken);

    private Task DeletePricingPolicyDirectAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("delete from pricing_policies where pricing_policy_id = @pricingPolicyId;", connection, transaction);
                command.Parameters.AddWithValue("@pricingPolicyId", pricingPolicyId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private Task InactivatePricingPolicyDirectAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand(
                    """
                    update pricing_policies
                    set is_active = 0
                    where pricing_policy_id = @pricingPolicyId;
                    """,
                    connection,
                    transaction);
                command.Parameters.AddWithValue("@pricingPolicyId", pricingPolicyId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private async Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> GetSeasonalityEventProfilesDirectAsync(
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadSeasonalityEventProfilesDirectAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    private async Task<SeasonalityEventProfileRecord> GetSeasonalityEventProfileByIdDirectAsync(long seasonalityEventProfileId, CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadSeasonalityEventProfileByIdDirectAsync(connection, null, seasonalityEventProfileId, cancellationToken);
    }

    private Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileDirectAsync(SeasonalityEventProfileRecord profile, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertSeasonalityEventProfileInternalDirectAsync(connection, transaction, profile, ct),
            cancellationToken);

    private Task DeleteSeasonalityEventProfileDirectAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("delete from seasonality_event_profiles where seasonality_event_profile_id = @seasonalityEventProfileId;", connection, transaction);
                command.Parameters.AddWithValue("@seasonalityEventProfileId", seasonalityEventProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private Task InactivateSeasonalityEventProfileDirectAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand(
                    """
                    update seasonality_event_profiles
                    set is_active = 0
                    where seasonality_event_profile_id = @seasonalityEventProfileId;
                    """,
                    connection,
                    transaction);
                command.Parameters.AddWithValue("@seasonalityEventProfileId", seasonalityEventProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private async Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> GetVendorSupplyProfilesDirectAsync(
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadVendorSupplyProfilesDirectAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    private async Task<VendorSupplyProfileRecord> GetVendorSupplyProfileByIdDirectAsync(long vendorSupplyProfileId, CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await LoadVendorSupplyProfileByIdDirectAsync(connection, null, vendorSupplyProfileId, cancellationToken);
    }

    private Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileDirectAsync(VendorSupplyProfileRecord profile, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertVendorSupplyProfileInternalDirectAsync(connection, transaction, profile, ct),
            cancellationToken);

    private Task DeleteVendorSupplyProfileDirectAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("delete from vendor_supply_profiles where vendor_supply_profile_id = @vendorSupplyProfileId;", connection, transaction);
                command.Parameters.AddWithValue("@vendorSupplyProfileId", vendorSupplyProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private Task InactivateVendorSupplyProfileDirectAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand(
                    """
                    update vendor_supply_profiles
                    set is_active = 0
                    where vendor_supply_profile_id = @vendorSupplyProfileId;
                    """,
                    connection,
                    transaction);
                command.Parameters.AddWithValue("@vendorSupplyProfileId", vendorSupplyProfileId);
                await command.ExecuteNonQueryAsync(ct);
            },
            cancellationToken);

    private static async Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> LoadInventoryProfilesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = new NpgsqlCommand(
            normalizedSearch is null
                ? "select count(*) from inventory_profiles;"
                : """
                  select count(*)
                  from inventory_profiles
                  where store_code ilike @searchTerm
                     or product_code ilike @searchTerm;
                  """,
            connection,
            transaction);
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
                ? """
                  select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                         safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
                  from inventory_profiles
                  order by store_code, product_code
                  limit @limit offset @offset;
                  """
                : """
                  select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                         safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
                  from inventory_profiles
                  where store_code ilike @searchTerm
                     or product_code ilike @searchTerm
                  order by store_code, product_code
                  limit @limit offset @offset;
                  """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@limit", currentPageSize);
        command.Parameters.AddWithValue("@offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var profiles = new List<InventoryProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadInventoryProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> LoadPricingPoliciesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = new NpgsqlCommand(
            normalizedSearch is null
                ? "select count(*) from pricing_policies;"
                : """
                  select count(*)
                  from pricing_policies
                  where coalesce(department, '') ilike @searchTerm
                     or coalesce(class_label, '') ilike @searchTerm
                     or coalesce(subclass, '') ilike @searchTerm
                     or coalesce(brand, '') ilike @searchTerm
                     or coalesce(price_ladder_group, '') ilike @searchTerm;
                  """,
            connection,
            transaction);
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
                ? """
                  select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                         markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
                  from pricing_policies
                  order by coalesce(department, ''), coalesce(class_label, ''), coalesce(subclass, ''),
                           coalesce(brand, ''), coalesce(price_ladder_group, '')
                  limit @limit offset @offset;
                  """
                : """
                  select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                         markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
                  from pricing_policies
                  where coalesce(department, '') ilike @searchTerm
                     or coalesce(class_label, '') ilike @searchTerm
                     or coalesce(subclass, '') ilike @searchTerm
                     or coalesce(brand, '') ilike @searchTerm
                     or coalesce(price_ladder_group, '') ilike @searchTerm
                  order by coalesce(department, ''), coalesce(class_label, ''), coalesce(subclass, ''),
                           coalesce(brand, ''), coalesce(price_ladder_group, '')
                  limit @limit offset @offset;
                  """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@limit", currentPageSize);
        command.Parameters.AddWithValue("@offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var policies = new List<PricingPolicyRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            policies.Add(ReadPricingPolicy(reader));
        }

        return (policies, totalCount);
    }

    private static async Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> LoadSeasonalityEventProfilesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = new NpgsqlCommand(
            normalizedSearch is null
                ? "select count(*) from seasonality_event_profiles;"
                : """
                  select count(*)
                  from seasonality_event_profiles
                  where coalesce(department, '') ilike @searchTerm
                     or coalesce(class_label, '') ilike @searchTerm
                     or coalesce(subclass, '') ilike @searchTerm
                     or coalesce(season_code, '') ilike @searchTerm
                     or coalesce(event_code, '') ilike @searchTerm
                     or coalesce(promo_window, '') ilike @searchTerm;
                  """,
            connection,
            transaction);
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
                ? """
                  select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                         peak_flag, is_active
                  from seasonality_event_profiles
                  order by month, coalesce(department, ''), coalesce(class_label, ''), coalesce(subclass, '')
                  limit @limit offset @offset;
                  """
                : """
                  select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                         peak_flag, is_active
                  from seasonality_event_profiles
                  where coalesce(department, '') ilike @searchTerm
                     or coalesce(class_label, '') ilike @searchTerm
                     or coalesce(subclass, '') ilike @searchTerm
                     or coalesce(season_code, '') ilike @searchTerm
                     or coalesce(event_code, '') ilike @searchTerm
                     or coalesce(promo_window, '') ilike @searchTerm
                  order by month, coalesce(department, ''), coalesce(class_label, ''), coalesce(subclass, '')
                  limit @limit offset @offset;
                  """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@limit", currentPageSize);
        command.Parameters.AddWithValue("@offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var profiles = new List<SeasonalityEventProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadSeasonalityEventProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> LoadVendorSupplyProfilesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = new NpgsqlCommand(
            normalizedSearch is null
                ? "select count(*) from vendor_supply_profiles;"
                : """
                  select count(*)
                  from vendor_supply_profiles
                  where supplier ilike @searchTerm
                     or coalesce(brand, '') ilike @searchTerm
                     or coalesce(replenishment_type, '') ilike @searchTerm
                     or coalesce(payment_terms, '') ilike @searchTerm;
                  """,
            connection,
            transaction);
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
                ? """
                  select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
                  from vendor_supply_profiles
                  order by supplier, coalesce(brand, '')
                  limit @limit offset @offset;
                  """
                : """
                  select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
                  from vendor_supply_profiles
                  where supplier ilike @searchTerm
                     or coalesce(brand, '') ilike @searchTerm
                     or coalesce(replenishment_type, '') ilike @searchTerm
                     or coalesce(payment_terms, '') ilike @searchTerm
                  order by supplier, coalesce(brand, '')
                  limit @limit offset @offset;
                  """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@limit", currentPageSize);
        command.Parameters.AddWithValue("@offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@searchTerm", normalizedSearch);
        }

        var profiles = new List<VendorSupplyProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadVendorSupplyProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<InventoryProfileRecord> UpsertInventoryProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        InventoryProfileRecord profile,
        CancellationToken cancellationToken)
    {
        if (profile.InventoryProfileId > 0)
        {
            await using var updateCommand = new NpgsqlCommand(
                """
                update inventory_profiles
                set store_code = @storeCode,
                    product_code = @productCode,
                    starting_inventory = @startingInventory,
                    inbound_qty = @inboundQty,
                    reserved_qty = @reservedQty,
                    projected_stock_on_hand = @projectedStockOnHand,
                    safety_stock = @safetyStock,
                    weeks_of_cover_target = @weeksOfCoverTarget,
                    sell_through_target_pct = @sellThroughTargetPct,
                    is_active = @isActive
                where inventory_profile_id = @inventoryProfileId;
                """,
                connection,
                transaction);
            BindInventoryProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadInventoryProfileByIdDirectAsync(connection, transaction, profile.InventoryProfileId, cancellationToken);
        }

        await using var insertCommand = new NpgsqlCommand(
            """
            insert into inventory_profiles (
                store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand, safety_stock,
                weeks_of_cover_target, sell_through_target_pct, is_active)
            values (
                @storeCode, @productCode, @startingInventory, @inboundQty, @reservedQty, @projectedStockOnHand, @safetyStock,
                @weeksOfCoverTarget, @sellThroughTargetPct, @isActive)
            returning inventory_profile_id;
            """,
            connection,
            transaction);
        BindInventoryProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadInventoryProfileByIdDirectAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<PricingPolicyRecord> UpsertPricingPolicyInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PricingPolicyRecord policy,
        CancellationToken cancellationToken)
    {
        if (policy.PricingPolicyId > 0)
        {
            await using var updateCommand = new NpgsqlCommand(
                """
                update pricing_policies
                set department = @department,
                    class_label = @classLabel,
                    subclass = @subclass,
                    brand = @brand,
                    price_ladder_group = @priceLadderGroup,
                    min_price = @minPrice,
                    max_price = @maxPrice,
                    markdown_floor_price = @markdownFloorPrice,
                    minimum_margin_pct = @minimumMarginPct,
                    kvi_flag = @kviFlag,
                    markdown_eligible = @markdownEligible,
                    is_active = @isActive
                where pricing_policy_id = @pricingPolicyId;
                """,
                connection,
                transaction);
            BindPricingPolicy(updateCommand, policy);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadPricingPolicyByIdDirectAsync(connection, transaction, policy.PricingPolicyId, cancellationToken);
        }

        await using var insertCommand = new NpgsqlCommand(
            """
            insert into pricing_policies (
                department, class_label, subclass, brand, price_ladder_group, min_price, max_price, markdown_floor_price,
                minimum_margin_pct, kvi_flag, markdown_eligible, is_active)
            values (
                @department, @classLabel, @subclass, @brand, @priceLadderGroup, @minPrice, @maxPrice, @markdownFloorPrice,
                @minimumMarginPct, @kviFlag, @markdownEligible, @isActive)
            returning pricing_policy_id;
            """,
            connection,
            transaction);
        BindPricingPolicy(insertCommand, policy);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadPricingPolicyByIdDirectAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        SeasonalityEventProfileRecord profile,
        CancellationToken cancellationToken)
    {
        if (profile.SeasonalityEventProfileId > 0)
        {
            await using var updateCommand = new NpgsqlCommand(
                """
                update seasonality_event_profiles
                set department = @department,
                    class_label = @classLabel,
                    subclass = @subclass,
                    season_code = @seasonCode,
                    event_code = @eventCode,
                    month = @month,
                    weight = @weight,
                    promo_window = @promoWindow,
                    peak_flag = @peakFlag,
                    is_active = @isActive
                where seasonality_event_profile_id = @seasonalityEventProfileId;
                """,
                connection,
                transaction);
            BindSeasonalityEventProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadSeasonalityEventProfileByIdDirectAsync(connection, transaction, profile.SeasonalityEventProfileId, cancellationToken);
        }

        await using var insertCommand = new NpgsqlCommand(
            """
            insert into seasonality_event_profiles (
                department, class_label, subclass, season_code, event_code, month, weight, promo_window, peak_flag, is_active)
            values (
                @department, @classLabel, @subclass, @seasonCode, @eventCode, @month, @weight, @promoWindow, @peakFlag, @isActive)
            returning seasonality_event_profile_id;
            """,
            connection,
            transaction);
        BindSeasonalityEventProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadSeasonalityEventProfileByIdDirectAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        VendorSupplyProfileRecord profile,
        CancellationToken cancellationToken)
    {
        if (profile.VendorSupplyProfileId > 0)
        {
            await using var updateCommand = new NpgsqlCommand(
                """
                update vendor_supply_profiles
                set supplier = @supplier,
                    brand = @brand,
                    lead_time_days = @leadTimeDays,
                    moq = @moq,
                    case_pack = @casePack,
                    replenishment_type = @replenishmentType,
                    payment_terms = @paymentTerms,
                    is_active = @isActive
                where vendor_supply_profile_id = @vendorSupplyProfileId;
                """,
                connection,
                transaction);
            BindVendorSupplyProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadVendorSupplyProfileByIdDirectAsync(connection, transaction, profile.VendorSupplyProfileId, cancellationToken);
        }

        await using var insertCommand = new NpgsqlCommand(
            """
            insert into vendor_supply_profiles (
                supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active)
            values (
                @supplier, @brand, @leadTimeDays, @moq, @casePack, @replenishmentType, @paymentTerms, @isActive)
            returning vendor_supply_profile_id;
            """,
            connection,
            transaction);
        BindVendorSupplyProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadVendorSupplyProfileByIdDirectAsync(connection, transaction, newId, cancellationToken);
    }

    private static void BindInventoryProfile(NpgsqlCommand command, InventoryProfileRecord profile)
    {
        command.Parameters.AddWithValue("@inventoryProfileId", profile.InventoryProfileId);
        command.Parameters.AddWithValue("@storeCode", profile.StoreCode);
        command.Parameters.AddWithValue("@productCode", profile.ProductCode);
        command.Parameters.AddWithValue("@startingInventory", profile.StartingInventory);
        command.Parameters.AddWithValue("@inboundQty", (object?)profile.InboundQty ?? DBNull.Value);
        command.Parameters.AddWithValue("@reservedQty", (object?)profile.ReservedQty ?? DBNull.Value);
        command.Parameters.AddWithValue("@projectedStockOnHand", (object?)profile.ProjectedStockOnHand ?? DBNull.Value);
        command.Parameters.AddWithValue("@safetyStock", (object?)profile.SafetyStock ?? DBNull.Value);
        command.Parameters.AddWithValue("@weeksOfCoverTarget", (object?)profile.WeeksOfCoverTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("@sellThroughTargetPct", (object?)profile.SellThroughTargetPct ?? DBNull.Value);
        command.Parameters.AddWithValue("@isActive", profile.IsActive ? 1 : 0);
    }

    private static void BindPricingPolicy(NpgsqlCommand command, PricingPolicyRecord policy)
    {
        command.Parameters.AddWithValue("@pricingPolicyId", policy.PricingPolicyId);
        command.Parameters.AddWithValue("@department", (object?)policy.Department ?? DBNull.Value);
        command.Parameters.AddWithValue("@classLabel", (object?)policy.ClassLabel ?? DBNull.Value);
        command.Parameters.AddWithValue("@subclass", (object?)policy.Subclass ?? DBNull.Value);
        command.Parameters.AddWithValue("@brand", (object?)policy.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("@priceLadderGroup", (object?)policy.PriceLadderGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("@minPrice", (object?)policy.MinPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxPrice", (object?)policy.MaxPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("@markdownFloorPrice", (object?)policy.MarkdownFloorPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("@minimumMarginPct", (object?)policy.MinimumMarginPct ?? DBNull.Value);
        command.Parameters.AddWithValue("@kviFlag", policy.KviFlag ? 1 : 0);
        command.Parameters.AddWithValue("@markdownEligible", policy.MarkdownEligible ? 1 : 0);
        command.Parameters.AddWithValue("@isActive", policy.IsActive ? 1 : 0);
    }

    private static void BindSeasonalityEventProfile(NpgsqlCommand command, SeasonalityEventProfileRecord profile)
    {
        command.Parameters.AddWithValue("@seasonalityEventProfileId", profile.SeasonalityEventProfileId);
        command.Parameters.AddWithValue("@department", (object?)profile.Department ?? DBNull.Value);
        command.Parameters.AddWithValue("@classLabel", (object?)profile.ClassLabel ?? DBNull.Value);
        command.Parameters.AddWithValue("@subclass", (object?)profile.Subclass ?? DBNull.Value);
        command.Parameters.AddWithValue("@seasonCode", (object?)profile.SeasonCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@eventCode", (object?)profile.EventCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@month", profile.Month);
        command.Parameters.AddWithValue("@weight", profile.Weight);
        command.Parameters.AddWithValue("@promoWindow", (object?)profile.PromoWindow ?? DBNull.Value);
        command.Parameters.AddWithValue("@peakFlag", profile.PeakFlag ? 1 : 0);
        command.Parameters.AddWithValue("@isActive", profile.IsActive ? 1 : 0);
    }

    private static void BindVendorSupplyProfile(NpgsqlCommand command, VendorSupplyProfileRecord profile)
    {
        command.Parameters.AddWithValue("@vendorSupplyProfileId", profile.VendorSupplyProfileId);
        command.Parameters.AddWithValue("@supplier", profile.Supplier);
        command.Parameters.AddWithValue("@brand", (object?)profile.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("@leadTimeDays", (object?)profile.LeadTimeDays ?? DBNull.Value);
        command.Parameters.AddWithValue("@moq", (object?)profile.Moq ?? DBNull.Value);
        command.Parameters.AddWithValue("@casePack", (object?)profile.CasePack ?? DBNull.Value);
        command.Parameters.AddWithValue("@replenishmentType", (object?)profile.ReplenishmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("@paymentTerms", (object?)profile.PaymentTerms ?? DBNull.Value);
        command.Parameters.AddWithValue("@isActive", profile.IsActive ? 1 : 0);
    }

    private static async Task<InventoryProfileRecord> LoadInventoryProfileByIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long inventoryProfileId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                   safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
            from inventory_profiles
            where inventory_profile_id = @inventoryProfileId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@inventoryProfileId", inventoryProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Inventory profile '{inventoryProfileId}' was not found.");
        }

        return ReadInventoryProfile(reader);
    }

    private static async Task<PricingPolicyRecord> LoadPricingPolicyByIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long pricingPolicyId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                   markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
            from pricing_policies
            where pricing_policy_id = @pricingPolicyId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@pricingPolicyId", pricingPolicyId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Pricing policy '{pricingPolicyId}' was not found.");
        }

        return ReadPricingPolicy(reader);
    }

    private static async Task<SeasonalityEventProfileRecord> LoadSeasonalityEventProfileByIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long seasonalityEventProfileId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                   peak_flag, is_active
            from seasonality_event_profiles
            where seasonality_event_profile_id = @seasonalityEventProfileId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@seasonalityEventProfileId", seasonalityEventProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Seasonality profile '{seasonalityEventProfileId}' was not found.");
        }

        return ReadSeasonalityEventProfile(reader);
    }

    private static async Task<VendorSupplyProfileRecord> LoadVendorSupplyProfileByIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long vendorSupplyProfileId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
            from vendor_supply_profiles
            where vendor_supply_profile_id = @vendorSupplyProfileId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@vendorSupplyProfileId", vendorSupplyProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Vendor supply profile '{vendorSupplyProfileId}' was not found.");
        }

        return ReadVendorSupplyProfile(reader);
    }

    private static InventoryProfileRecord ReadInventoryProfile(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetString(2),
        ReadDecimal(reader, 3),
        reader.IsDBNull(4) ? null : ReadDecimal(reader, 4),
        reader.IsDBNull(5) ? null : ReadDecimal(reader, 5),
        reader.IsDBNull(6) ? null : ReadDecimal(reader, 6),
        reader.IsDBNull(7) ? null : ReadDecimal(reader, 7),
        reader.IsDBNull(8) ? null : ReadDecimal(reader, 8),
        reader.IsDBNull(9) ? null : ReadDecimal(reader, 9),
        ReadFlag(reader, 10));

    private static PricingPolicyRecord ReadPricingPolicy(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.IsDBNull(1) ? null : reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.IsDBNull(4) ? null : reader.GetString(4),
        reader.IsDBNull(5) ? null : reader.GetString(5),
        reader.IsDBNull(6) ? null : ReadDecimal(reader, 6),
        reader.IsDBNull(7) ? null : ReadDecimal(reader, 7),
        reader.IsDBNull(8) ? null : ReadDecimal(reader, 8),
        reader.IsDBNull(9) ? null : ReadDecimal(reader, 9),
        ReadFlag(reader, 10),
        ReadFlag(reader, 11),
        ReadFlag(reader, 12));

    private static SeasonalityEventProfileRecord ReadSeasonalityEventProfile(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.IsDBNull(1) ? null : reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.IsDBNull(4) ? null : reader.GetString(4),
        reader.IsDBNull(5) ? null : reader.GetString(5),
        reader.GetInt32(6),
        ReadDecimal(reader, 7),
        reader.IsDBNull(8) ? null : reader.GetString(8),
        ReadFlag(reader, 9),
        ReadFlag(reader, 10));

    private static VendorSupplyProfileRecord ReadVendorSupplyProfile(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
        reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
        reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
        reader.IsDBNull(6) ? null : reader.GetString(6),
        reader.IsDBNull(7) ? null : reader.GetString(7),
        ReadFlag(reader, 8));

    private static decimal ReadDecimal(NpgsqlDataReader reader, int ordinal) =>
        Convert.ToDecimal(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static bool ReadFlag(NpgsqlDataReader reader, int ordinal) =>
        Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture) != 0;
}
