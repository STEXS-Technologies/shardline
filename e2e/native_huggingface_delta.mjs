import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

setTimeout(() => {
  console.error(`official JavaScript delta workflow exceeded 120 seconds; stage=${stage}; last HTTP=${lastHttp}`);
  process.exit(124);
}, 120_000).unref();

const require = createRequire(resolve(process.env.SHARDLINE_HF_JS_ROOT, "package.json"));
const manifest = require(resolve(process.env.SHARDLINE_HF_JS_ROOT, "node_modules/@huggingface/hub/package.json"));
assert.equal(manifest.version, "2.17.1", "official client version must match qualification");
const { createRepo, uploadFile, downloadFile } = await import(pathToFileURL(require.resolve("@huggingface/hub")));
const nativeFetch = globalThis.fetch.bind(globalThis);
let deltaRequests = 0;
let tokenRefreshes = 0;
let initialXorbBytes = 0;
let deltaXorbBytes = 0;
let lastHttp = "none";
let stage = "setup";
globalThis.fetch = async (input, init) => {
  const url = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
  if (process.env.SHARDLINE_TRACE_JS_HTTP === "1") console.error(`request stage=${stage}; ${url.pathname}`);
  if (url.pathname.includes("/v2/file-chunk-hashes/")) deltaRequests++;
  if (url.pathname.includes("/xet-write-token/")) tokenRefreshes++;
  if (url.pathname.startsWith("/v1/xorbs/default/")) {
    const bytes = init?.body?.size ?? init?.body?.byteLength;
    assert(Number.isSafeInteger(bytes) && bytes > 0, "Xorb payload size must be observable without consuming it");
    if (stage === "initial native upload") initialXorbBytes += bytes;
    if (stage === "delta edit") deltaXorbBytes += bytes;
  }
  const response = await nativeFetch(input, init);
  lastHttp = `${url.pathname}:${response.status}`;
  if (process.env.SHARDLINE_TRACE_JS_HTTP === "1") console.error(`response ${lastHttp}`);
  if (process.env.SHARDLINE_TRACE_JS_HTTP === "1" && response.status >= 500) {
    const body = await response.clone().text();
    console.error(`server failure storage_full=${/StorageFull|InsufficientStorage|minimum.*free|storage.*full/i.test(body)}`);
  }
  return response;
};
const repoType = process.env.SHARDLINE_JS_REPO_TYPE ?? "model";
assert(["model", "dataset"].includes(repoType));
const params = {
  repo: { type: repoType, name: repoType === "model" ? "team/cli-model" : "team/cli-dataset" },
  hubUrl: process.env.SHARDLINE_ENDPOINT,
  accessToken: process.env.SHARDLINE_TOKEN,
};
try {
  stage = "create repository";
  await createRepo({ ...params, private: true });
  const content = new Uint8Array(9 * 1024 * 1024);
  let state = 0x12345678;
  for (let index = 0; index < content.length; index++) {
    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
    content[index] = state & 255;
  }
  stage = "initial native upload";
  await uploadFile({ ...params, useXet: true, file: { path: "weights.bin", content: new Blob([content]) } });
  stage = "resolve native original";
  const original = await downloadFile({ ...params, path: "weights.bin", xet: true });
  assert(original);
  const replacement = new Uint8Array(32 * 1024).fill(0xa7);
  stage = "delta edit";
  await uploadFile({ ...params, useXet: true, file: {
    path: "weights.bin", originalContent: original,
    edits: [{ start: 1024 * 1024, end: 1024 * 1024 + replacement.length, content: new Blob([replacement]) }],
  } });
  assert(deltaRequests > 0, "official client must select the delta endpoint");
  assert(tokenRefreshes > 0, "official client must successfully exercise token refresh");
  assert(deltaXorbBytes > 0 && deltaXorbBytes < initialXorbBytes,
    "this sparse edit must upload fewer Xorb payload bytes than the initial file");
  stage = "verify edited download";
  content.set(replacement, 1024 * 1024);
  const result = await downloadFile({ ...params, path: "weights.bin", xet: true });
  assert(result);
  if (process.env.SHARDLINE_TRACE_JS_HTTP === "1") {
    console.error(`edited blob size=${result.size}`);
  }
  assert.deepEqual(new Uint8Array(await result.arrayBuffer()), content);
  stage = "completed";
  if (process.env.SHARDLINE_TRACE_JS_HTTP === "1") console.error("all workflow assertions passed");
  await new Promise((resolve, reject) => process.stdout.write(
    `delta verified; delta requests=${deltaRequests}; initial Xorb bytes=${initialXorbBytes}; edited Xorb bytes=${deltaXorbBytes}\n`,
    error => error ? reject(error) : resolve(),
  ));
  // This is a one-shot client subprocess. All protocol and byte-integrity
  // assertions above are awaited; SDK background handles must not turn a
  // completed workflow into the watchdog's failure. Handle cleanup and
  // cancellation require their own tests rather than this process-exit gate.
  process.exit(0);
} catch (error) {
  // Do not print credential-bearing client errors, URLs or headers.
  console.error(`stage=${stage}; error=${error?.name ?? "Error"}; last HTTP=${lastHttp}`);
  process.exit(1);
}
