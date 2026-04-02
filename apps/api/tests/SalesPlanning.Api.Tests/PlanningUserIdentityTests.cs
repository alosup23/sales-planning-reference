using System.Security.Claims;
using SalesPlanning.Api.Security;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PlanningUserIdentityTests
{
    [Fact]
    public void ResolvePlanningUserToken_PreservesCandidateAliasesAndUsesPreferredUsernameAsPrimary()
    {
        var principal = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim("name", "Planner Display Name"),
            new Claim("preferred_username", "planner@example.com"),
            new Claim("oid", "11111111-2222-3333-4444-555555555555"),
        ], "Bearer"));

        var token = PlanningUserIdentity.ResolvePlanningUserToken(principal, authEnabled: true);
        var context = PlanningUserIdentity.ParsePlanningUserToken(token);

        Assert.Equal("planner@example.com", context.PrimaryUserId);
        Assert.Contains("planner@example.com", context.CandidateUserIds);
        Assert.Contains("11111111-2222-3333-4444-555555555555", context.CandidateUserIds);
    }

    [Fact]
    public void ResolveRequiredUserId_UsesLocalTestUserWhenAuthDisabled()
    {
        var principal = new ClaimsPrincipal(new ClaimsIdentity());

        var resolved = PlanningUserIdentity.ResolveRequiredUserId(principal, authEnabled: false);

        Assert.Equal("local.test.user", resolved);
    }

    [Fact]
    public void ParsePlanningUserToken_TreatsPlainUserIdsAsSingleCandidateContext()
    {
        var context = PlanningUserIdentity.ParsePlanningUserToken("planner.one");

        Assert.Equal("planner.one", context.PrimaryUserId);
        Assert.Equal(["planner.one"], context.CandidateUserIds);
    }
}
