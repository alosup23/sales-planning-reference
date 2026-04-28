import { EventType, InteractionRequiredAuthError, PublicClientApplication, type AuthenticationResult, type Configuration } from "@azure/msal-browser";
import { ensurePlanningEnvironmentReady, isPlanningEnvironmentControlEnabled } from "./environmentControl";

const authMode = import.meta.env.VITE_AUTH_MODE ?? "entra";
const clientId = import.meta.env.VITE_ENTRA_CLIENT_ID ?? "557f0c81-0531-4616-b62e-0b69eb7cb86f";
const defaultApiScope = `api://${clientId}/SalesPlanning.Access`;

export const authEnabled = authMode !== "disabled";
const configuredApiScope = import.meta.env.VITE_ENTRA_API_SCOPE?.trim();
const apiScope = configuredApiScope || defaultApiScope;
export const loginRequest = {
  scopes: Array.from(new Set(["User.Read", apiScope])),
};
export const apiRequest = {
  scopes: [apiScope],
};

const redirectUri =
  import.meta.env.VITE_ENTRA_REDIRECT_URI ??
  (typeof window !== "undefined" ? window.location.origin : "http://localhost:5173");

const postLogoutRedirectUri =
  import.meta.env.VITE_ENTRA_POST_LOGOUT_REDIRECT_URI ??
  (typeof window !== "undefined" ? window.location.origin : "http://localhost:5173");

const msalConfig: Configuration = {
  auth: {
    clientId,
    authority: `https://login.microsoftonline.com/${import.meta.env.VITE_ENTRA_TENANT_ID ?? "76ad236c-6db1-4d3d-9901-996450816c3c"}`,
    redirectUri,
    postLogoutRedirectUri,
  },
  cache: {
    cacheLocation: "sessionStorage",
  },
};

export const msalInstance = authEnabled ? new PublicClientApplication(msalConfig) : null;

async function primePlanningEnvironment(accessToken: string | null): Promise<void> {
  if (!accessToken || !isPlanningEnvironmentControlEnabled()) {
    return;
  }

  try {
    await ensurePlanningEnvironmentReady(accessToken);
  } catch (error) {
    console.warn("Unable to warm the planning environment during sign-in.", error);
  }
}

export async function initializeAuth(): Promise<void> {
  if (!msalInstance) {
    return;
  }

  await msalInstance.initialize();
  msalInstance.addEventCallback((event) => {
    if (event.eventType !== EventType.LOGIN_SUCCESS || !event.payload) {
      return;
    }

    const authenticationResult = event.payload as AuthenticationResult;
    msalInstance.setActiveAccount(authenticationResult.account);
    void primePlanningEnvironment(authenticationResult.accessToken || null);
  });

  const redirectResult = await msalInstance.handleRedirectPromise();
  if (redirectResult?.account) {
    msalInstance.setActiveAccount(redirectResult.account);
    void primePlanningEnvironment(redirectResult.accessToken || null);
    return;
  }

  const activeAccount = msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;
  if (activeAccount) {
    msalInstance.setActiveAccount(activeAccount);
    try {
      const result = await msalInstance.acquireTokenSilent({
        ...apiRequest,
        account: activeAccount,
      });
      void primePlanningEnvironment(result.accessToken || null);
    } catch (error) {
      if (!(error instanceof InteractionRequiredAuthError)) {
        console.warn("Unable to pre-warm the planning environment from the existing session.", error);
      }
    }
  }
}

export async function getAccessToken(): Promise<string | null> {
  if (!msalInstance) {
    return null;
  }

  const account = msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;
  if (!account) {
    await msalInstance.loginRedirect(loginRequest);
    throw new Error("Redirecting to Microsoft 365 sign-in...");
  }

  try {
    const result = await msalInstance.acquireTokenSilent({
      ...apiRequest,
      account,
    });

    return result.accessToken || null;
  } catch (error) {
    if (error instanceof InteractionRequiredAuthError) {
      await msalInstance.acquireTokenRedirect({
        ...apiRequest,
        account,
      });
      throw new Error("Refreshing Microsoft 365 session...");
    }

    throw error;
  }
}
