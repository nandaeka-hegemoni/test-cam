import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join, dirname, extname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 8765;
const FFMPEG = process.env.FFMPEG_PATH || "ffmpeg";
const FFPROBE = process.env.FFPROBE_PATH || "ffprobe";

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

function resolveRtspUrl(rawUrl, user, pass) {
  const trimmed = (rawUrl || "").trim();
  if (!trimmed) throw new Error("URL RTSP kosong.");
  if (!/^rtsp:\/\//i.test(trimmed)) {
    throw new Error("URL harus diawali rtsp://");
  }

  const u = new URL(trimmed);
  if (!u.username && user) u.username = user;
  if (!u.password && pass) u.password = pass;
  return u.href;
}

function redactRtspUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    if (u.password) u.password = "***";
    if (u.username) u.username = u.username ? "***" : "";
    return u.href;
  } catch {
    return rawUrl;
  }
}

function runProcess(bin, args, timeoutMs) {
  return new Promise((resolve, reject) => {
    const proc = spawn(bin, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      proc.kill("SIGKILL");
      reject(new Error(`Timeout setelah ${timeoutMs}ms`));
    }, timeoutMs);

    proc.stdout?.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    proc.on("error", (err) => {
      clearTimeout(timer);
      if (err.code === "ENOENT") {
        reject(
          new Error(
            `${bin} tidak ditemukan. Install FFmpeg (brew install ffmpeg).`
          )
        );
        return;
      }
      reject(err);
    });
    proc.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0) resolve({ stdout, stderr });
      else {
        const msg = (stderr || stdout || `exit ${code}`).trim();
        reject(new Error(msg.slice(-800)));
      }
    });
  });
}

async function probeRtsp(rtspUrl, transport) {
  const { stdout } = await runProcess(
    FFPROBE,
    [
      "-hide_banner",
      "-loglevel",
      "error",
      "-rtsp_transport",
      transport,
      "-i",
      rtspUrl,
      "-show_entries",
      "stream=index,codec_type,codec_name,width,height",
      "-of",
      "json",
    ],
    20000
  );

  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    parsed = { streams: [] };
  }

  const streams = parsed.streams || [];
  const video = streams.find((s) => s.codec_type === "video");

  return {
    ok: true,
    streamCount: streams.length,
    videoCodec: video?.codec_name ?? null,
    width: video?.width ?? null,
    height: video?.height ?? null,
  };
}

function pipeRtspAsMjpeg(rtspUrl, transport, res, req) {
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
    "-rtsp_transport",
    transport,
    "-i",
    rtspUrl,
    "-an",
    "-f",
    "mpjpeg",
    "-q",
    "5",
    "pipe:1",
  ];

  const proc = spawn(FFMPEG, args, { stdio: ["ignore", "pipe", "pipe"] });
  let started = false;

  const fail = (message, status = 502) => {
    if (started) return;
    started = true;
    res.writeHead(status, { "Content-Type": "text/plain; charset=utf-8" });
    res.end(message);
    proc.kill("SIGKILL");
  };

  proc.stderr?.on("data", (chunk) => {
    const text = chunk.toString();
    if (!started && /401|403|Unauthorized|Forbidden/i.test(text)) {
      fail("Auth RTSP ditolak (cek username/password).", 401);
    }
  });

  proc.on("error", (err) => {
    if (err.code === "ENOENT") {
      fail("ffmpeg tidak ditemukan. Install FFmpeg di mesin yang menjalankan server.");
      return;
    }
    fail(err.message || String(err));
  });

  proc.stdout?.once("data", () => {
    if (started) return;
    started = true;
    res.writeHead(200, {
      "Content-Type": 'multipart/x-mixed-replace; boundary="ffmpeg"',
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "Access-Control-Allow-Origin": "*",
      Connection: "close",
    });
  });

  proc.stdout?.pipe(res);

  const cleanup = () => {
    proc.kill("SIGKILL");
  };
  req.on("close", cleanup);
  res.on("close", cleanup);
  proc.on("close", (code) => {
    if (!started && code !== 0) {
      fail("Gagal membuka stream RTSP. Cek URL, port, path, dan transport (tcp/udp).");
    }
  });
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

function json(res, status, body) {
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(JSON.stringify(body));
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

  if (u.pathname === "/api/rtsp-probe" && req.method === "GET") {
    const rawUrl = u.searchParams.get("url");
    const user = u.searchParams.get("user") ?? "";
    const pass = u.searchParams.get("pass") ?? "";
    const transport = u.searchParams.get("transport") === "udp" ? "udp" : "tcp";

    if (!rawUrl) {
      json(res, 400, { ok: false, error: "missing url" });
      return;
    }

    let rtspUrl;
    try {
      rtspUrl = resolveRtspUrl(rawUrl, user, pass);
    } catch (err) {
      json(res, 400, { ok: false, error: err.message });
      return;
    }

    try {
      const info = await probeRtsp(rtspUrl, transport);
      json(res, 200, {
        via: "ffmpeg/ffprobe",
        url: redactRtspUrl(rtspUrl),
        transport,
        ...info,
      });
    } catch (err) {
      json(res, 502, {
        ok: false,
        url: redactRtspUrl(rtspUrl),
        transport,
        error: err.message || String(err),
      });
    }
    return;
  }

  if (u.pathname === "/api/rtsp-stream" && req.method === "GET") {
    const rawUrl = u.searchParams.get("url");
    const user = u.searchParams.get("user") ?? "";
    const pass = u.searchParams.get("pass") ?? "";
    const transport = u.searchParams.get("transport") === "udp" ? "udp" : "tcp";

    if (!rawUrl) {
      res.writeHead(400);
      res.end("missing url");
      return;
    }

    let rtspUrl;
    try {
      rtspUrl = resolveRtspUrl(rawUrl, user, pass);
    } catch (err) {
      res.writeHead(400);
      res.end(err.message);
      return;
    }

    pipeRtspAsMjpeg(rtspUrl, transport, res, req);
    return;
  }

  if (u.pathname === "/api/probe" && req.method === "GET") {
    const target = u.searchParams.get("url");
    const user = u.searchParams.get("user") ?? "";
    const pass = u.searchParams.get("pass") ?? "";
    if (!target) {
      json(res, 400, { error: "missing url" });
      return;
    }
    try {
      const info = await probeUpstream(target, user, pass);
      json(res, 200, { url: target, via: "proxy", ...info });
    } catch (err) {
      json(res, 502, {
        url: target,
        ok: false,
        error: err.message || String(err),
      });
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
  console.log(`  HTTP probe:  /api/probe?url=...&user=...&pass=...`);
  console.log(`  RTSP probe:  /api/rtsp-probe?url=rtsp://...&user=...&pass=...`);
  console.log(`  RTSP preview: /api/rtsp-stream?url=rtsp://... (butuh FFmpeg)`);
});
