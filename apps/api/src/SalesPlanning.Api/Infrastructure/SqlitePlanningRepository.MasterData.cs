using System.Globalization;
using Microsoft.Data.Sqlite;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed partial class SqlitePlanningRepository
{
    public async Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> GetInventoryProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadInventoryProfilesAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    public async Task<InventoryProfileRecord> UpsertInventoryProfileAsync(InventoryProfileRecord profile, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var upserted = await UpsertInventoryProfileInternalAsync(connection, transaction, profile, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return upserted;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<InventoryProfileRecord> GetInventoryProfileByIdAsync(long inventoryProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadInventoryProfileByIdAsync(connection, null, inventoryProfileId, cancellationToken);
    }

    public async Task DeleteInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "delete from inventory_profiles where inventory_profile_id = $inventoryProfileId;";
            command.Parameters.AddWithValue("$inventoryProfileId", inventoryProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivateInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                update inventory_profiles
                set is_active = 0
                where inventory_profile_id = $inventoryProfileId;
                """;
            command.Parameters.AddWithValue("$inventoryProfileId", inventoryProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> GetPricingPoliciesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadPricingPoliciesAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    public async Task<PricingPolicyRecord> UpsertPricingPolicyAsync(PricingPolicyRecord policy, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var upserted = await UpsertPricingPolicyInternalAsync(connection, transaction, policy, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return upserted;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<PricingPolicyRecord> GetPricingPolicyByIdAsync(long pricingPolicyId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadPricingPolicyByIdAsync(connection, null, pricingPolicyId, cancellationToken);
    }

    public async Task DeletePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "delete from pricing_policies where pricing_policy_id = $pricingPolicyId;";
            command.Parameters.AddWithValue("$pricingPolicyId", pricingPolicyId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivatePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                update pricing_policies
                set is_active = 0
                where pricing_policy_id = $pricingPolicyId;
                """;
            command.Parameters.AddWithValue("$pricingPolicyId", pricingPolicyId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> GetSeasonalityEventProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadSeasonalityEventProfilesAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    public async Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileAsync(SeasonalityEventProfileRecord profile, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var upserted = await UpsertSeasonalityEventProfileInternalAsync(connection, transaction, profile, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return upserted;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<SeasonalityEventProfileRecord> GetSeasonalityEventProfileByIdAsync(long seasonalityEventProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadSeasonalityEventProfileByIdAsync(connection, null, seasonalityEventProfileId, cancellationToken);
    }

    public async Task DeleteSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "delete from seasonality_event_profiles where seasonality_event_profile_id = $seasonalityEventProfileId;";
            command.Parameters.AddWithValue("$seasonalityEventProfileId", seasonalityEventProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivateSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                update seasonality_event_profiles
                set is_active = 0
                where seasonality_event_profile_id = $seasonalityEventProfileId;
                """;
            command.Parameters.AddWithValue("$seasonalityEventProfileId", seasonalityEventProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> GetVendorSupplyProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadVendorSupplyProfilesAsync(connection, null, searchTerm, pageNumber, pageSize, cancellationToken);
    }

    public async Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileAsync(VendorSupplyProfileRecord profile, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var upserted = await UpsertVendorSupplyProfileInternalAsync(connection, transaction, profile, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return upserted;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<VendorSupplyProfileRecord> GetVendorSupplyProfileByIdAsync(long vendorSupplyProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadVendorSupplyProfileByIdAsync(connection, null, vendorSupplyProfileId, cancellationToken);
    }

    public async Task DeleteVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "delete from vendor_supply_profiles where vendor_supply_profile_id = $vendorSupplyProfileId;";
            command.Parameters.AddWithValue("$vendorSupplyProfileId", vendorSupplyProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivateVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                update vendor_supply_profiles
                set is_active = 0
                where vendor_supply_profile_id = $vendorSupplyProfileId;
                """;
            command.Parameters.AddWithValue("$vendorSupplyProfileId", vendorSupplyProfileId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async Task EnsureAdditionalMasterDataTablesAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            create table if not exists inventory_profiles (
                inventory_profile_id integer primary key autoincrement,
                store_code text not null,
                product_code text not null,
                starting_inventory real not null,
                inbound_qty real null,
                reserved_qty real null,
                projected_stock_on_hand real null,
                safety_stock real null,
                weeks_of_cover_target real null,
                sell_through_target_pct real null,
                is_active integer not null default 1
            );
            create table if not exists pricing_policies (
                pricing_policy_id integer primary key autoincrement,
                department text null,
                class_label text null,
                subclass text null,
                brand text null,
                price_ladder_group text null,
                min_price real null,
                max_price real null,
                markdown_floor_price real null,
                minimum_margin_pct real null,
                kvi_flag integer not null default 0,
                markdown_eligible integer not null default 1,
                is_active integer not null default 1
            );
            create table if not exists seasonality_event_profiles (
                seasonality_event_profile_id integer primary key autoincrement,
                department text null,
                class_label text null,
                subclass text null,
                season_code text null,
                event_code text null,
                month integer not null,
                weight real not null,
                promo_window text null,
                peak_flag integer not null default 0,
                is_active integer not null default 1
            );
            create table if not exists vendor_supply_profiles (
                vendor_supply_profile_id integer primary key autoincrement,
                supplier text not null,
                brand text null,
                lead_time_days integer null,
                moq integer null,
                case_pack integer null,
                replenishment_type text null,
                payment_terms text null,
                is_active integer not null default 1
            );
            create index if not exists idx_inventory_profiles_lookup on inventory_profiles (store_code, product_code);
            create index if not exists idx_pricing_policies_lookup on pricing_policies (department, class_label, subclass, brand, price_ladder_group);
            create index if not exists idx_seasonality_profiles_lookup on seasonality_event_profiles (department, class_label, subclass, season_code, event_code, month);
            create index if not exists idx_vendor_supply_profiles_lookup on vendor_supply_profiles (supplier, brand);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> LoadInventoryProfilesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = connection.CreateCommand();
        countCommand.Transaction = transaction;
        countCommand.CommandText = normalizedSearch is null
            ? "select count(*) from inventory_profiles;"
            : """
                select count(*)
                from inventory_profiles
                where store_code like $searchTerm collate nocase
                   or product_code like $searchTerm collate nocase;
                """;
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? """
                select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                       safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
                from inventory_profiles
                order by store_code collate nocase, product_code collate nocase
                limit $limit offset $offset;
                """
            : """
                select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                       safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
                from inventory_profiles
                where store_code like $searchTerm collate nocase
                   or product_code like $searchTerm collate nocase
                order by store_code collate nocase, product_code collate nocase
                limit $limit offset $offset;
                """;
        command.Parameters.AddWithValue("$limit", currentPageSize);
        command.Parameters.AddWithValue("$offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var profiles = new List<InventoryProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadInventoryProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> LoadPricingPoliciesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = connection.CreateCommand();
        countCommand.Transaction = transaction;
        countCommand.CommandText = normalizedSearch is null
            ? "select count(*) from pricing_policies;"
            : """
                select count(*)
                from pricing_policies
                where ifnull(department, '') like $searchTerm collate nocase
                   or ifnull(class_label, '') like $searchTerm collate nocase
                   or ifnull(subclass, '') like $searchTerm collate nocase
                   or ifnull(brand, '') like $searchTerm collate nocase
                   or ifnull(price_ladder_group, '') like $searchTerm collate nocase;
                """;
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? """
                select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                       markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
                from pricing_policies
                order by ifnull(department, '') collate nocase, ifnull(class_label, '') collate nocase, ifnull(subclass, '') collate nocase,
                         ifnull(brand, '') collate nocase, ifnull(price_ladder_group, '') collate nocase
                limit $limit offset $offset;
                """
            : """
                select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                       markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
                from pricing_policies
                where ifnull(department, '') like $searchTerm collate nocase
                   or ifnull(class_label, '') like $searchTerm collate nocase
                   or ifnull(subclass, '') like $searchTerm collate nocase
                   or ifnull(brand, '') like $searchTerm collate nocase
                   or ifnull(price_ladder_group, '') like $searchTerm collate nocase
                order by ifnull(department, '') collate nocase, ifnull(class_label, '') collate nocase, ifnull(subclass, '') collate nocase,
                         ifnull(brand, '') collate nocase, ifnull(price_ladder_group, '') collate nocase
                limit $limit offset $offset;
                """;
        command.Parameters.AddWithValue("$limit", currentPageSize);
        command.Parameters.AddWithValue("$offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var policies = new List<PricingPolicyRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            policies.Add(ReadPricingPolicy(reader));
        }

        return (policies, totalCount);
    }

    private static async Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> LoadSeasonalityEventProfilesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = connection.CreateCommand();
        countCommand.Transaction = transaction;
        countCommand.CommandText = normalizedSearch is null
            ? "select count(*) from seasonality_event_profiles;"
            : """
                select count(*)
                from seasonality_event_profiles
                where ifnull(department, '') like $searchTerm collate nocase
                   or ifnull(class_label, '') like $searchTerm collate nocase
                   or ifnull(subclass, '') like $searchTerm collate nocase
                   or ifnull(season_code, '') like $searchTerm collate nocase
                   or ifnull(event_code, '') like $searchTerm collate nocase
                   or ifnull(promo_window, '') like $searchTerm collate nocase;
                """;
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? """
                select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                       peak_flag, is_active
                from seasonality_event_profiles
                order by month, ifnull(department, '') collate nocase, ifnull(class_label, '') collate nocase, ifnull(subclass, '') collate nocase
                limit $limit offset $offset;
                """
            : """
                select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                       peak_flag, is_active
                from seasonality_event_profiles
                where ifnull(department, '') like $searchTerm collate nocase
                   or ifnull(class_label, '') like $searchTerm collate nocase
                   or ifnull(subclass, '') like $searchTerm collate nocase
                   or ifnull(season_code, '') like $searchTerm collate nocase
                   or ifnull(event_code, '') like $searchTerm collate nocase
                   or ifnull(promo_window, '') like $searchTerm collate nocase
                order by month, ifnull(department, '') collate nocase, ifnull(class_label, '') collate nocase, ifnull(subclass, '') collate nocase
                limit $limit offset $offset;
                """;
        command.Parameters.AddWithValue("$limit", currentPageSize);
        command.Parameters.AddWithValue("$offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var profiles = new List<SeasonalityEventProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadSeasonalityEventProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> LoadVendorSupplyProfilesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var currentPage = Math.Max(1, pageNumber);
        var currentPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (currentPage - 1) * currentPageSize;

        await using var countCommand = connection.CreateCommand();
        countCommand.Transaction = transaction;
        countCommand.CommandText = normalizedSearch is null
            ? "select count(*) from vendor_supply_profiles;"
            : """
                select count(*)
                from vendor_supply_profiles
                where supplier like $searchTerm collate nocase
                   or ifnull(brand, '') like $searchTerm collate nocase
                   or ifnull(replenishment_type, '') like $searchTerm collate nocase
                   or ifnull(payment_terms, '') like $searchTerm collate nocase;
                """;
        if (normalizedSearch is not null)
        {
            countCommand.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? """
                select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
                from vendor_supply_profiles
                order by supplier collate nocase, ifnull(brand, '') collate nocase
                limit $limit offset $offset;
                """
            : """
                select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
                from vendor_supply_profiles
                where supplier like $searchTerm collate nocase
                   or ifnull(brand, '') like $searchTerm collate nocase
                   or ifnull(replenishment_type, '') like $searchTerm collate nocase
                   or ifnull(payment_terms, '') like $searchTerm collate nocase
                order by supplier collate nocase, ifnull(brand, '') collate nocase
                limit $limit offset $offset;
                """;
        command.Parameters.AddWithValue("$limit", currentPageSize);
        command.Parameters.AddWithValue("$offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$searchTerm", normalizedSearch);
        }

        var profiles = new List<VendorSupplyProfileRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            profiles.Add(ReadVendorSupplyProfile(reader));
        }

        return (profiles, totalCount);
    }

    private static async Task<InventoryProfileRecord> UpsertInventoryProfileInternalAsync(SqliteConnection connection, SqliteTransaction transaction, InventoryProfileRecord profile, CancellationToken cancellationToken)
    {
        if (profile.InventoryProfileId > 0)
        {
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = """
                update inventory_profiles
                set store_code = $storeCode,
                    product_code = $productCode,
                    starting_inventory = $startingInventory,
                    inbound_qty = $inboundQty,
                    reserved_qty = $reservedQty,
                    projected_stock_on_hand = $projectedStockOnHand,
                    safety_stock = $safetyStock,
                    weeks_of_cover_target = $weeksOfCoverTarget,
                    sell_through_target_pct = $sellThroughTargetPct,
                    is_active = $isActive
                where inventory_profile_id = $inventoryProfileId;
                """;
            BindInventoryProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadInventoryProfileByIdAsync(connection, transaction, profile.InventoryProfileId, cancellationToken);
        }

        await using var insertCommand = connection.CreateCommand();
        insertCommand.Transaction = transaction;
        insertCommand.CommandText = """
            insert into inventory_profiles (
                store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand, safety_stock,
                weeks_of_cover_target, sell_through_target_pct, is_active)
            values (
                $storeCode, $productCode, $startingInventory, $inboundQty, $reservedQty, $projectedStockOnHand, $safetyStock,
                $weeksOfCoverTarget, $sellThroughTargetPct, $isActive);
            select last_insert_rowid();
            """;
        BindInventoryProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadInventoryProfileByIdAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<PricingPolicyRecord> UpsertPricingPolicyInternalAsync(SqliteConnection connection, SqliteTransaction transaction, PricingPolicyRecord policy, CancellationToken cancellationToken)
    {
        if (policy.PricingPolicyId > 0)
        {
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = """
                update pricing_policies
                set department = $department,
                    class_label = $classLabel,
                    subclass = $subclass,
                    brand = $brand,
                    price_ladder_group = $priceLadderGroup,
                    min_price = $minPrice,
                    max_price = $maxPrice,
                    markdown_floor_price = $markdownFloorPrice,
                    minimum_margin_pct = $minimumMarginPct,
                    kvi_flag = $kviFlag,
                    markdown_eligible = $markdownEligible,
                    is_active = $isActive
                where pricing_policy_id = $pricingPolicyId;
                """;
            BindPricingPolicy(updateCommand, policy);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadPricingPolicyByIdAsync(connection, transaction, policy.PricingPolicyId, cancellationToken);
        }

        await using var insertCommand = connection.CreateCommand();
        insertCommand.Transaction = transaction;
        insertCommand.CommandText = """
            insert into pricing_policies (
                department, class_label, subclass, brand, price_ladder_group, min_price, max_price, markdown_floor_price,
                minimum_margin_pct, kvi_flag, markdown_eligible, is_active)
            values (
                $department, $classLabel, $subclass, $brand, $priceLadderGroup, $minPrice, $maxPrice, $markdownFloorPrice,
                $minimumMarginPct, $kviFlag, $markdownEligible, $isActive);
            select last_insert_rowid();
            """;
        BindPricingPolicy(insertCommand, policy);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadPricingPolicyByIdAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileInternalAsync(SqliteConnection connection, SqliteTransaction transaction, SeasonalityEventProfileRecord profile, CancellationToken cancellationToken)
    {
        if (profile.SeasonalityEventProfileId > 0)
        {
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = """
                update seasonality_event_profiles
                set department = $department,
                    class_label = $classLabel,
                    subclass = $subclass,
                    season_code = $seasonCode,
                    event_code = $eventCode,
                    month = $month,
                    weight = $weight,
                    promo_window = $promoWindow,
                    peak_flag = $peakFlag,
                    is_active = $isActive
                where seasonality_event_profile_id = $seasonalityEventProfileId;
                """;
            BindSeasonalityEventProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadSeasonalityEventProfileByIdAsync(connection, transaction, profile.SeasonalityEventProfileId, cancellationToken);
        }

        await using var insertCommand = connection.CreateCommand();
        insertCommand.Transaction = transaction;
        insertCommand.CommandText = """
            insert into seasonality_event_profiles (
                department, class_label, subclass, season_code, event_code, month, weight, promo_window, peak_flag, is_active)
            values (
                $department, $classLabel, $subclass, $seasonCode, $eventCode, $month, $weight, $promoWindow, $peakFlag, $isActive);
            select last_insert_rowid();
            """;
        BindSeasonalityEventProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadSeasonalityEventProfileByIdAsync(connection, transaction, newId, cancellationToken);
    }

    private static async Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileInternalAsync(SqliteConnection connection, SqliteTransaction transaction, VendorSupplyProfileRecord profile, CancellationToken cancellationToken)
    {
        if (profile.VendorSupplyProfileId > 0)
        {
            await using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = """
                update vendor_supply_profiles
                set supplier = $supplier,
                    brand = $brand,
                    lead_time_days = $leadTimeDays,
                    moq = $moq,
                    case_pack = $casePack,
                    replenishment_type = $replenishmentType,
                    payment_terms = $paymentTerms,
                    is_active = $isActive
                where vendor_supply_profile_id = $vendorSupplyProfileId;
                """;
            BindVendorSupplyProfile(updateCommand, profile);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
            return await LoadVendorSupplyProfileByIdAsync(connection, transaction, profile.VendorSupplyProfileId, cancellationToken);
        }

        await using var insertCommand = connection.CreateCommand();
        insertCommand.Transaction = transaction;
        insertCommand.CommandText = """
            insert into vendor_supply_profiles (
                supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active)
            values (
                $supplier, $brand, $leadTimeDays, $moq, $casePack, $replenishmentType, $paymentTerms, $isActive);
            select last_insert_rowid();
            """;
        BindVendorSupplyProfile(insertCommand, profile);
        var newId = Convert.ToInt64(await insertCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        return await LoadVendorSupplyProfileByIdAsync(connection, transaction, newId, cancellationToken);
    }

    private static void BindInventoryProfile(SqliteCommand command, InventoryProfileRecord profile)
    {
        command.Parameters.AddWithValue("$inventoryProfileId", profile.InventoryProfileId);
        command.Parameters.AddWithValue("$storeCode", profile.StoreCode);
        command.Parameters.AddWithValue("$productCode", profile.ProductCode);
        command.Parameters.AddWithValue("$startingInventory", profile.StartingInventory);
        command.Parameters.AddWithValue("$inboundQty", (object?)profile.InboundQty ?? DBNull.Value);
        command.Parameters.AddWithValue("$reservedQty", (object?)profile.ReservedQty ?? DBNull.Value);
        command.Parameters.AddWithValue("$projectedStockOnHand", (object?)profile.ProjectedStockOnHand ?? DBNull.Value);
        command.Parameters.AddWithValue("$safetyStock", (object?)profile.SafetyStock ?? DBNull.Value);
        command.Parameters.AddWithValue("$weeksOfCoverTarget", (object?)profile.WeeksOfCoverTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("$sellThroughTargetPct", (object?)profile.SellThroughTargetPct ?? DBNull.Value);
        command.Parameters.AddWithValue("$isActive", profile.IsActive ? 1 : 0);
    }

    private static void BindPricingPolicy(SqliteCommand command, PricingPolicyRecord policy)
    {
        command.Parameters.AddWithValue("$pricingPolicyId", policy.PricingPolicyId);
        command.Parameters.AddWithValue("$department", (object?)policy.Department ?? DBNull.Value);
        command.Parameters.AddWithValue("$classLabel", (object?)policy.ClassLabel ?? DBNull.Value);
        command.Parameters.AddWithValue("$subclass", (object?)policy.Subclass ?? DBNull.Value);
        command.Parameters.AddWithValue("$brand", (object?)policy.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("$priceLadderGroup", (object?)policy.PriceLadderGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$minPrice", (object?)policy.MinPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("$maxPrice", (object?)policy.MaxPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("$markdownFloorPrice", (object?)policy.MarkdownFloorPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("$minimumMarginPct", (object?)policy.MinimumMarginPct ?? DBNull.Value);
        command.Parameters.AddWithValue("$kviFlag", policy.KviFlag ? 1 : 0);
        command.Parameters.AddWithValue("$markdownEligible", policy.MarkdownEligible ? 1 : 0);
        command.Parameters.AddWithValue("$isActive", policy.IsActive ? 1 : 0);
    }

    private static void BindSeasonalityEventProfile(SqliteCommand command, SeasonalityEventProfileRecord profile)
    {
        command.Parameters.AddWithValue("$seasonalityEventProfileId", profile.SeasonalityEventProfileId);
        command.Parameters.AddWithValue("$department", (object?)profile.Department ?? DBNull.Value);
        command.Parameters.AddWithValue("$classLabel", (object?)profile.ClassLabel ?? DBNull.Value);
        command.Parameters.AddWithValue("$subclass", (object?)profile.Subclass ?? DBNull.Value);
        command.Parameters.AddWithValue("$seasonCode", (object?)profile.SeasonCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$eventCode", (object?)profile.EventCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$month", profile.Month);
        command.Parameters.AddWithValue("$weight", profile.Weight);
        command.Parameters.AddWithValue("$promoWindow", (object?)profile.PromoWindow ?? DBNull.Value);
        command.Parameters.AddWithValue("$peakFlag", profile.PeakFlag ? 1 : 0);
        command.Parameters.AddWithValue("$isActive", profile.IsActive ? 1 : 0);
    }

    private static void BindVendorSupplyProfile(SqliteCommand command, VendorSupplyProfileRecord profile)
    {
        command.Parameters.AddWithValue("$vendorSupplyProfileId", profile.VendorSupplyProfileId);
        command.Parameters.AddWithValue("$supplier", profile.Supplier);
        command.Parameters.AddWithValue("$brand", (object?)profile.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("$leadTimeDays", (object?)profile.LeadTimeDays ?? DBNull.Value);
        command.Parameters.AddWithValue("$moq", (object?)profile.Moq ?? DBNull.Value);
        command.Parameters.AddWithValue("$casePack", (object?)profile.CasePack ?? DBNull.Value);
        command.Parameters.AddWithValue("$replenishmentType", (object?)profile.ReplenishmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("$paymentTerms", (object?)profile.PaymentTerms ?? DBNull.Value);
        command.Parameters.AddWithValue("$isActive", profile.IsActive ? 1 : 0);
    }

    private static async Task<InventoryProfileRecord> LoadInventoryProfileByIdAsync(SqliteConnection connection, SqliteTransaction? transaction, long inventoryProfileId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select inventory_profile_id, store_code, product_code, starting_inventory, inbound_qty, reserved_qty, projected_stock_on_hand,
                   safety_stock, weeks_of_cover_target, sell_through_target_pct, is_active
            from inventory_profiles
            where inventory_profile_id = $inventoryProfileId;
            """;
        command.Parameters.AddWithValue("$inventoryProfileId", inventoryProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Inventory profile '{inventoryProfileId}' was not found.");
        }

        return ReadInventoryProfile(reader);
    }

    private static async Task<PricingPolicyRecord> LoadPricingPolicyByIdAsync(SqliteConnection connection, SqliteTransaction? transaction, long pricingPolicyId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select pricing_policy_id, department, class_label, subclass, brand, price_ladder_group, min_price, max_price,
                   markdown_floor_price, minimum_margin_pct, kvi_flag, markdown_eligible, is_active
            from pricing_policies
            where pricing_policy_id = $pricingPolicyId;
            """;
        command.Parameters.AddWithValue("$pricingPolicyId", pricingPolicyId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Pricing policy '{pricingPolicyId}' was not found.");
        }

        return ReadPricingPolicy(reader);
    }

    private static async Task<SeasonalityEventProfileRecord> LoadSeasonalityEventProfileByIdAsync(SqliteConnection connection, SqliteTransaction? transaction, long seasonalityEventProfileId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select seasonality_event_profile_id, department, class_label, subclass, season_code, event_code, month, weight, promo_window,
                   peak_flag, is_active
            from seasonality_event_profiles
            where seasonality_event_profile_id = $seasonalityEventProfileId;
            """;
        command.Parameters.AddWithValue("$seasonalityEventProfileId", seasonalityEventProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Seasonality profile '{seasonalityEventProfileId}' was not found.");
        }

        return ReadSeasonalityEventProfile(reader);
    }

    private static async Task<VendorSupplyProfileRecord> LoadVendorSupplyProfileByIdAsync(SqliteConnection connection, SqliteTransaction? transaction, long vendorSupplyProfileId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select vendor_supply_profile_id, supplier, brand, lead_time_days, moq, case_pack, replenishment_type, payment_terms, is_active
            from vendor_supply_profiles
            where vendor_supply_profile_id = $vendorSupplyProfileId;
            """;
        command.Parameters.AddWithValue("$vendorSupplyProfileId", vendorSupplyProfileId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Vendor supply profile '{vendorSupplyProfileId}' was not found.");
        }

        return ReadVendorSupplyProfile(reader);
    }

    private static InventoryProfileRecord ReadInventoryProfile(SqliteDataReader reader) => new(
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
        !reader.IsDBNull(10) && reader.GetInt64(10) == 1);

    private static PricingPolicyRecord ReadPricingPolicy(SqliteDataReader reader) => new(
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
        !reader.IsDBNull(10) && reader.GetInt64(10) == 1,
        !reader.IsDBNull(11) && reader.GetInt64(11) == 1,
        !reader.IsDBNull(12) && reader.GetInt64(12) == 1);

    private static SeasonalityEventProfileRecord ReadSeasonalityEventProfile(SqliteDataReader reader) => new(
        reader.GetInt64(0),
        reader.IsDBNull(1) ? null : reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.IsDBNull(4) ? null : reader.GetString(4),
        reader.IsDBNull(5) ? null : reader.GetString(5),
        reader.GetInt32(6),
        ReadDecimal(reader, 7),
        reader.IsDBNull(8) ? null : reader.GetString(8),
        !reader.IsDBNull(9) && reader.GetInt64(9) == 1,
        !reader.IsDBNull(10) && reader.GetInt64(10) == 1);

    private static VendorSupplyProfileRecord ReadVendorSupplyProfile(SqliteDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.IsDBNull(3) ? null : reader.GetInt32(3),
        reader.IsDBNull(4) ? null : reader.GetInt32(4),
        reader.IsDBNull(5) ? null : reader.GetInt32(5),
        reader.IsDBNull(6) ? null : reader.GetString(6),
        reader.IsDBNull(7) ? null : reader.GetString(7),
        !reader.IsDBNull(8) && reader.GetInt64(8) == 1);
}
