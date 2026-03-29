using System.Security.Claims;
using SalesPlanning.Api.Infrastructure.Security;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class EntraApiAccessPolicyTests
{
    [Fact]
    public void HasPlanningApiAccess_AcceptsShortScope()
    {
        var principal = BuildPrincipal(("scp", "SalesPlanning.Access"));

        var result = EntraApiAccessPolicy.HasPlanningApiAccess(
            principal,
            "api://557f0c81-0531-4616-b62e-0b69eb7cb86f/SalesPlanning.Access");

        Assert.True(result);
    }

    [Fact]
    public void HasPlanningApiAccess_AcceptsFullScopeValue()
    {
        var principal = BuildPrincipal(("scp", "api://557f0c81-0531-4616-b62e-0b69eb7cb86f/SalesPlanning.Access"));

        var result = EntraApiAccessPolicy.HasPlanningApiAccess(
            principal,
            "api://557f0c81-0531-4616-b62e-0b69eb7cb86f/SalesPlanning.Access");

        Assert.True(result);
    }

    [Fact]
    public void HasPlanningApiAccess_AcceptsRoleClaims()
    {
        var principal = BuildPrincipal(("roles", "SalesPlanning.Admin"));

        var result = EntraApiAccessPolicy.HasPlanningApiAccess(
            principal,
            "SalesPlanning.Access");

        Assert.True(result);
    }

    [Fact]
    public void HasPlanningApiAccess_RejectsMissingScopeAndRole()
    {
        var principal = BuildPrincipal(("scp", "User.Read"));

        var result = EntraApiAccessPolicy.HasPlanningApiAccess(
            principal,
            "SalesPlanning.Access");

        Assert.False(result);
    }

    private static ClaimsPrincipal BuildPrincipal(params (string Type, string Value)[] claims) =>
        new(new ClaimsIdentity(claims.Select(claim => new Claim(claim.Type, claim.Value)), "Test"));
}
