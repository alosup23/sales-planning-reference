import React from "react";
import ReactDOM from "react-dom/client";
import { MsalProvider } from "@azure/msal-react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import { AuthShell } from "./components/AuthShell";
import { authEnabled, initializeAuth, msalInstance } from "./lib/auth";
import "./styles/app.css";

const queryClient = new QueryClient();

async function bootstrap() {
  if (authEnabled) {
    await initializeAuth();
  }

  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <QueryClientProvider client={queryClient}>
        {authEnabled && msalInstance ? (
          <MsalProvider instance={msalInstance}>
            <AuthShell>
              <App />
            </AuthShell>
          </MsalProvider>
        ) : (
          <App />
        )}
      </QueryClientProvider>
    </React.StrictMode>,
  );
}

void bootstrap();
