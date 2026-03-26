import { useEffect, useMemo, useState, type PropsWithChildren } from "react";
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

  const initials = useMemo(() => {
    const source = (account.name ?? account.username ?? "U").trim();
    const parts = source.split(/\s+/).filter(Boolean);
    if (parts.length === 0) {
      return "U";
    }

    if (parts.length === 1) {
      return parts[0].slice(0, 2).toUpperCase();
    }

    return `${parts[0][0] ?? ""}${parts[1][0] ?? ""}`.toUpperCase();
  }, [account.name, account.username]);

  useEffect(() => {
    if (!menuOpen) {
      return;
    }

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || target.closest(".auth-user-menu")) {
        return;
      }

      setMenuOpen(false);
    };

    window.addEventListener("pointerdown", handlePointerDown);
    return () => window.removeEventListener("pointerdown", handlePointerDown);
  }, [menuOpen]);

  return (
    <div className="auth-user-menu">
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
