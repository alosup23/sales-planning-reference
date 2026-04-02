using System.Security.Claims;
using SalesPlanning.Api.Security;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PlanningUserIdentityTests
{
    [Fact]
    public void ResolveRequiredUserId_PrefersImmutableOidOverDisplayName()
    {
        var principal = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim("name", "Planner Display Name"),
            new Claim("preferred_username", "planner@example.com"),
            new Claim("oid", "11111111-2222-3333-4444-555555555555"),
        ], "Bearer"));

        var resolved = PlanningUserIdentity.ResolveRequiredUserId(principal, authEnabled: true);

        Assert.Equal("11111111-2222-3333-4444-555555555555", resolved);
    }

    [Fact]
    public void ResolveRequiredUserId_UsesLocalTestUserWhenAuthDisabled()
    {
        var principal = new ClaimsPrincipal(new ClaimsIdentity());

        var resolved = PlanningUserIdentity.ResolveRequiredUserId(principal, authEnabled: false);

        Assert.Equal("local.test.user", resolved);
    }
}
