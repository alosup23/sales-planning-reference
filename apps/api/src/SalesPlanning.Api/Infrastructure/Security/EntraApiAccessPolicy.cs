using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;

namespace SalesPlanning.Api.Infrastructure.Security;

public static class EntraApiAccessPolicy
{
    public const string PolicyName = "PlanningApiAccess";

    public static AuthorizationPolicy Build(string requiredScope) =>
        new AuthorizationPolicyBuilder()
            .RequireAuthenticatedUser()
            .RequireAssertion(context => HasPlanningApiAccess(context.User, requiredScope))
            .Build();

    public static bool HasPlanningApiAccess(ClaimsPrincipal user, string requiredScope)
    {
        if (user.Claims.Any(claim =>
                string.Equals(claim.Type, "roles", StringComparison.OrdinalIgnoreCase)
                || string.Equals(claim.Type, ClaimTypes.Role, StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        var normalizedRequiredScope = requiredScope.Trim();
        if (string.IsNullOrWhiteSpace(normalizedRequiredScope))
        {
            return false;
        }

        var shortScope = normalizedRequiredScope.Contains('/', StringComparison.Ordinal)
            ? normalizedRequiredScope[(normalizedRequiredScope.LastIndexOf('/') + 1)..]
            : normalizedRequiredScope;

        return user.Claims
            .Where(claim =>
                string.Equals(claim.Type, "scp", StringComparison.OrdinalIgnoreCase)
                || string.Equals(claim.Type, "scope", StringComparison.OrdinalIgnoreCase)
                || string.Equals(claim.Type, "http://schemas.microsoft.com/identity/claims/scope", StringComparison.OrdinalIgnoreCase))
            .SelectMany(claim => claim.Value.Split([' ', ','], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Any(scope =>
                string.Equals(scope, normalizedRequiredScope, StringComparison.OrdinalIgnoreCase)
                || string.Equals(scope, shortScope, StringComparison.OrdinalIgnoreCase));
    }
}
