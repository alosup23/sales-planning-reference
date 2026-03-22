import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const tempDirectory = path.resolve(__dirname, "../.tmp");
const keyPath = path.join(tempDirectory, "localhost-key.pem");
const certPath = path.join(tempDirectory, "localhost-cert.pem");

fs.mkdirSync(tempDirectory, { recursive: true });

if (!fs.existsSync(keyPath) || !fs.existsSync(certPath)) {
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "2",
      "-subj",
      "/CN=localhost",
    ],
    { stdio: "ignore" },
  );
}

const key = fs.readFileSync(keyPath);
const cert = fs.readFileSync(certPath);

const targetHost = "127.0.0.1";
const targetPort = Number(process.env.API_PROXY_TARGET_PORT ?? "5000");

const server = https.createServer({ key, cert }, (req, res) => {
  const upstream = http.request(
    {
      protocol: "http:",
      hostname: targetHost,
      port: targetPort,
      method: req.method,
      path: req.url,
      headers: req.headers,
    },
    (upstreamRes) => {
      res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
      upstreamRes.pipe(res);
    },
  );

  upstream.on("error", (error) => {
    res.writeHead(502, { "content-type": "text/plain" });
    res.end(`Proxy error: ${error.message}`);
  });

  req.pipe(upstream);
});

server.listen(7080, "localhost", () => {
  console.log(`HTTPS API proxy listening on https://localhost:7080 -> http://${targetHost}:${targetPort}`);
});
