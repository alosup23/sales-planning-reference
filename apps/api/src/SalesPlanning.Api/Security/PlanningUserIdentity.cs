using System.Security.Claims;
using System.Text;
using System.Text.Json;

namespace SalesPlanning.Api.Security;

public static class PlanningUserIdentity
{
    private const string PlanningUserTokenPrefix = "planning-user:";

    public sealed record PlanningUserContext(string PrimaryUserId, IReadOnlyList<string> CandidateUserIds);

    public static string ResolveRequiredUserId(ClaimsPrincipal user, bool authEnabled)
    {
        return ResolvePlanningUserContext(user, authEnabled).PrimaryUserId;
    }

    public static string ResolvePlanningUserToken(ClaimsPrincipal user, bool authEnabled)
    {
        return SerializePlanningUserContext(ResolvePlanningUserContext(user, authEnabled));
    }

    public static string SerializePlanningUserContext(PlanningUserContext context)
    {
        var normalizedCandidates = context.CandidateUserIds
            .Where(static candidate => !string.IsNullOrWhiteSpace(candidate))
            .Select(static candidate => candidate.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (!normalizedCandidates.Contains(context.PrimaryUserId, StringComparer.Ordinal))
        {
            normalizedCandidates.Insert(0, context.PrimaryUserId);
        }

        if (normalizedCandidates.Count == 1 && string.Equals(normalizedCandidates[0], context.PrimaryUserId, StringComparison.Ordinal))
        {
            return context.PrimaryUserId;
        }

        var payload = JsonSerializer.Serialize(new PlanningUserTokenPayload(context.PrimaryUserId, normalizedCandidates));
        return $"{PlanningUserTokenPrefix}{Convert.ToBase64String(Encoding.UTF8.GetBytes(payload))}";
    }

    public static PlanningUserContext ParsePlanningUserToken(string userToken)
    {
        if (!userToken.StartsWith(PlanningUserTokenPrefix, StringComparison.Ordinal))
        {
            var primaryUserId = userToken.Trim();
            return new PlanningUserContext(primaryUserId, [primaryUserId]);
        }

        var encodedPayload = userToken[PlanningUserTokenPrefix.Length..];
        var payload = JsonSerializer.Deserialize<PlanningUserTokenPayload>(Encoding.UTF8.GetString(Convert.FromBase64String(encodedPayload)))
            ?? throw new InvalidOperationException("Planning user token was invalid.");
        var primary = payload.PrimaryUserId.Trim();
        var candidates = payload.CandidateUserIds
            .Where(static candidate => !string.IsNullOrWhiteSpace(candidate))
            .Select(static candidate => candidate.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (!candidates.Contains(primary, StringComparer.Ordinal))
        {
            candidates.Insert(0, primary);
        }

        return new PlanningUserContext(primary, candidates);
    }

    public static PlanningUserContext CreatePlanningUserContext(string primaryUserId, params string[] candidateUserIds)
    {
        var candidates = candidateUserIds
            .Where(static candidate => !string.IsNullOrWhiteSpace(candidate))
            .Select(static candidate => candidate.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (!candidates.Contains(primaryUserId, StringComparer.Ordinal))
        {
            candidates.Insert(0, primaryUserId);
        }

        return new PlanningUserContext(primaryUserId.Trim(), candidates);
    }

    private static PlanningUserContext ResolvePlanningUserContext(ClaimsPrincipal user, bool authEnabled)
    {
        if (!authEnabled)
        {
            return new PlanningUserContext("local.test.user", ["local.test.user"]);
        }

        var candidates = new[]
            {
                user.FindFirst("preferred_username")?.Value,
                user.FindFirst("oid")?.Value,
                user.FindFirst(ClaimTypes.NameIdentifier)?.Value,
                user.FindFirst("sub")?.Value,
                user.FindFirst("name")?.Value,
                user.Identity?.Name,
            }
            .Where(static candidate => !string.IsNullOrWhiteSpace(candidate))
            .Select(static candidate => candidate!.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (candidates.Count == 0)
        {
            throw new UnauthorizedAccessException("Authenticated user identity was not available.");
        }

        return new PlanningUserContext(candidates[0], candidates);
    }

    private sealed record PlanningUserTokenPayload(string PrimaryUserId, IReadOnlyList<string> CandidateUserIds);
}
