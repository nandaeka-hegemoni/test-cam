import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { join, dirname, extname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 8765;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
};

function basicAuthHeader(user, pass) {
  if (!user && !pass) return {};
  return {
    Authorization:
      "Basic " + Buffer.from(`${user}:${pass}`, "utf8").toString("base64"),
  };
}

async function probeUpstream(targetUrl, user, pass) {
  const headers = basicAuthHeader(user, pass);
  let res;
  try {
    res = await fetch(targetUrl, {
      method: "HEAD",
      headers,
      redirect: "follow",
      signal: AbortSignal.timeout(8000),
    });
  } catch {
    res = null;
  }

  if (!res || res.status === 405 || res.status === 501) {
    res = await fetch(targetUrl, {
      method: "GET",
      headers: { ...headers, Range: "bytes=0-0" },
      redirect: "follow",
      signal: AbortSignal.timeout(8000),
    });
  }

  return {
    ok: res.ok,
    status: res.status,
    statusText: res.statusText,
    contentType: res.headers.get("content-type"),
    contentLength: res.headers.get("content-length"),
  };
}

async function pipeUpstream(targetUrl, user, pass, res) {
  const upstream = await fetch(targetUrl, {
    headers: basicAuthHeader(user, pass),
    redirect: "follow",
  });

  const ct =
    upstream.headers.get("content-type") || "application/octet-stream";
  res.writeHead(upstream.status, {
    "Content-Type": ct,
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*",
  });

  if (!upstream.ok || !upstream.body) {
    res.end(await upstream.text().catch(() => ""));
    return;
  }

  for await (const chunk of upstream.body) {
    res.write(chunk);
  }
  res.end();
}

const server = createServer(async (req, res) => {
  const host = req.headers.host || `127.0.0.1:${PORT}`;
  const u = new URL(req.url || "/", `http://${host}`);

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    res.end();
    return;
  }

  if (u.pathname === "/api/probe" && req.method === "GET") {
    const target = u.searchParams.get("url");
    const user = u.searchParams.get("user") ?? "";
    const pass = u.searchParams.get("pass") ?? "";
    if (!target) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "missing url" }));
      return;
    }
    try {
      const info = await probeUpstream(target, user, pass);
      res.writeHead(200, {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      });
      res.end(JSON.stringify({ url: target, via: "proxy", ...info }));
    } catch (err) {
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          url: target,
          ok: false,
          error: err.message || String(err),
        })
      );
    }
    return;
  }

  if (u.pathname === "/api/proxy" && req.method === "GET") {
    const target = u.searchParams.get("url");
    const user = u.searchParams.get("user") ?? "";
    const pass = u.searchParams.get("pass") ?? "";
    if (!target) {
      res.writeHead(400);
      res.end("missing url");
      return;
    }
    try {
      await pipeUpstream(target, user, pass, res);
    } catch (err) {
      res.writeHead(502);
      res.end(err.message || String(err));
    }
    return;
  }

  let filePath = join(ROOT, u.pathname === "/" ? "index.html" : u.pathname);
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  try {
    const data = await readFile(filePath);
    res.writeHead(200, {
      "Content-Type": MIME[extname(filePath)] || "application/octet-stream",
    });
    res.end(data);
  } catch {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`test-cam: http://127.0.0.1:${PORT}`);
  console.log(`  proxy probe: /api/probe?url=...&user=...&pass=...`);
});
