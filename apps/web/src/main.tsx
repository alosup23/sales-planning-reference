import React, { type ErrorInfo, type ReactNode } from "react";
import ReactDOM from "react-dom/client";
import { MsalProvider } from "@azure/msal-react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import { AuthShell } from "./components/AuthShell";
import { authEnabled, initializeAuth, msalInstance } from "./lib/auth";
import "./styles/app.css";

const queryClient = new QueryClient();

class AppErrorBoundary extends React.Component<{ children: ReactNode }, { error: Error | null }> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Sales planning app crashed during render.", error, errorInfo);
  }

  render() {
    if (!this.state.error) {
      return this.props.children;
    }

    return (
      <main className="app-shell">
        <section className="hero">
          <div>
            <div className="eyebrow">Enterprise planning skeleton</div>
            <h1>Sales Budget &amp; Planning</h1>
            <p>
              The planning workspace hit a runtime error while loading. Refresh once and retry. If it persists, capture this
              message for support.
            </p>
          </div>
          <div className="hero-sidecar">
            <div className="status-card status-card-error" aria-live="assertive">
              {this.state.error.message || "Unexpected application error."}
            </div>
          </div>
        </section>
      </main>
    );
  }
}

async function bootstrap() {
  if (authEnabled) {
    await initializeAuth();
  }

  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <QueryClientProvider client={queryClient}>
        <AppErrorBoundary>
          {authEnabled && msalInstance ? (
            <MsalProvider instance={msalInstance}>
              <AuthShell>
                <App />
              </AuthShell>
            </MsalProvider>
          ) : (
            <App />
          )}
        </AppErrorBoundary>
      </QueryClientProvider>
    </React.StrictMode>,
  );
}

void bootstrap();
