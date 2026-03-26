import type { PropsWithChildren } from "react";
import { AuthenticatedTemplate, UnauthenticatedTemplate, useMsal } from "@azure/msal-react";
import { loginRequest } from "../lib/auth";

export function AuthShell({ children }: PropsWithChildren) {
  const { instance } = useMsal();

  return (
    <>
      <UnauthenticatedTemplate>
        <main className="auth-shell">
          <section className="auth-card">
            <div className="eyebrow">Microsoft 365 secure access</div>
            <h1>Sales Budget &amp; Planning</h1>
            <p>
              Sign in with your Microsoft 365 work account to access the planning workspace, hierarchy maintenance sheet,
              and forecasting insights.
            </p>
            <div className="auth-actions">
              <button
                type="button"
                className="secondary-button secondary-button-active"
                onClick={() => void instance.loginRedirect(loginRequest)}
              >
                Sign in with Microsoft 365
              </button>
            </div>
          </section>
        </main>
      </UnauthenticatedTemplate>
      <AuthenticatedTemplate>{children}</AuthenticatedTemplate>
    </>
  );
}

export function SignedInUserBadge() {
  const { accounts, instance } = useMsal();
  const account = instance.getActiveAccount() ?? accounts[0] ?? null;

  if (!account) {
    return null;
  }

  return (
    <div className="auth-user-card">
      <div className="eyebrow">Signed in</div>
      <strong>{account.name ?? account.username}</strong>
      <span>{account.username}</span>
      <button
        type="button"
        className="secondary-button"
        onClick={() => void instance.logoutRedirect()}
      >
        Sign out
      </button>
    </div>
  );
}
