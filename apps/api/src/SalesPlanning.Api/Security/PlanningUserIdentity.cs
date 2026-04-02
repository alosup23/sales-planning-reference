using System.Security.Claims;

namespace SalesPlanning.Api.Security;

public static class PlanningUserIdentity
{
    public static string ResolveRequiredUserId(ClaimsPrincipal user, bool authEnabled)
    {
        if (!authEnabled)
        {
            return "local.test.user";
        }

        return FirstNonEmpty(
            user.FindFirst("oid")?.Value,
            user.FindFirst(ClaimTypes.NameIdentifier)?.Value,
            user.FindFirst("sub")?.Value,
            user.FindFirst("preferred_username")?.Value,
            user.FindFirst("name")?.Value,
            user.Identity?.Name)
            ?? throw new UnauthorizedAccessException("Authenticated user identity was not available.");
    }

    private static string? FirstNonEmpty(params string?[] candidates)
    {
        foreach (var candidate in candidates)
        {
            if (!string.IsNullOrWhiteSpace(candidate))
            {
                return candidate.Trim();
            }
        }

        return null;
    }
}
