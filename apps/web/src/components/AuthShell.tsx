import { useState, type FocusEvent, type PropsWithChildren } from "react";
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

export function SignedInUserMenu() {
  const { accounts, instance } = useMsal();
  const account = instance.getActiveAccount() ?? accounts[0] ?? null;
  const [menuOpen, setMenuOpen] = useState(false);

  if (!account) {
    return null;
  }

  const source = (account.name ?? account.username ?? "U").trim();
  const parts = source.split(/\s+/).filter(Boolean);
  const initials = parts.length === 0
    ? "U"
    : parts.length === 1
      ? parts[0].slice(0, 2).toUpperCase()
      : `${parts[0][0] ?? ""}${parts[1][0] ?? ""}`.toUpperCase();

  const handleBlur = (event: FocusEvent<HTMLDivElement>) => {
    const nextTarget = event.relatedTarget;
    if (nextTarget instanceof Node && event.currentTarget.contains(nextTarget)) {
      return;
    }

    setMenuOpen(false);
  };

  return (
    <div className="auth-user-menu" onBlur={handleBlur}>
      <button
        type="button"
        className="profile-menu-button"
        aria-expanded={menuOpen}
        aria-haspopup="menu"
        onClick={() => setMenuOpen((current) => !current)}
      >
        <span className="profile-menu-avatar" aria-hidden="true">{initials}</span>
        <span className="profile-menu-name">{account.name ?? account.username}</span>
        <span className="profile-menu-caret" aria-hidden="true">{menuOpen ? "▴" : "▾"}</span>
      </button>
      {menuOpen ? (
        <div className="profile-menu-popover" role="menu">
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
      ) : null}
    </div>
  );
}
