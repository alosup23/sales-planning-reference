import { EventType, InteractionRequiredAuthError, PublicClientApplication, type AuthenticationResult, type Configuration } from "@azure/msal-browser";

const authMode = import.meta.env.VITE_AUTH_MODE ?? "entra";

export const authEnabled = authMode !== "disabled";
export const loginRequest = {
  scopes: ["User.Read"],
};
const configuredApiScope = import.meta.env.VITE_ENTRA_API_SCOPE?.trim();
export const apiRequest = {
  scopes: configuredApiScope ? [configuredApiScope] : ["User.Read"],
};

const redirectUri =
  import.meta.env.VITE_ENTRA_REDIRECT_URI ??
  (typeof window !== "undefined" ? window.location.origin : "http://localhost:5173");

const postLogoutRedirectUri =
  import.meta.env.VITE_ENTRA_POST_LOGOUT_REDIRECT_URI ??
  (typeof window !== "undefined" ? window.location.origin : "http://localhost:5173");

const msalConfig: Configuration = {
  auth: {
    clientId: import.meta.env.VITE_ENTRA_CLIENT_ID ?? "557f0c81-0531-4616-b62e-0b69eb7cb86f",
    authority: `https://login.microsoftonline.com/${import.meta.env.VITE_ENTRA_TENANT_ID ?? "76ad236c-6db1-4d3d-9901-996450816c3c"}`,
    redirectUri,
    postLogoutRedirectUri,
  },
  cache: {
    cacheLocation: "sessionStorage",
  },
};

export const msalInstance = authEnabled ? new PublicClientApplication(msalConfig) : null;

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
  });

  const redirectResult = await msalInstance.handleRedirectPromise();
  if (redirectResult?.account) {
    msalInstance.setActiveAccount(redirectResult.account);
    return;
  }

  const activeAccount = msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;
  if (activeAccount) {
    msalInstance.setActiveAccount(activeAccount);
  }
}

export async function getAccessToken(): Promise<string | null> {
  if (!msalInstance) {
    return null;
  }

  const account = msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;
  if (!account) {
    return null;
  }

  try {
    const result = await msalInstance.acquireTokenSilent({
      ...apiRequest,
      account,
    });

    return result.accessToken || null;
  } catch (error) {
    if (error instanceof InteractionRequiredAuthError) {
      throw new Error("Session expired. Sign in again.");
    }

    throw error;
  }
}
