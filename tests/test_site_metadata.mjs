import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import test from "node:test";
import sharp from "sharp";
import { Resvg } from "@resvg/resvg-js";
import { siteAddress, fitText, previewSvg } from "../site/.vitepress/metadata.mjs";

const template = await readFile(new URL("../site/social-preview.svg", import.meta.url), "utf8");

test("deployment addresses reject credentials, non-HTTPS URLs and path escapes", () => {
  assert.equal(siteAddress("/", "https://examples.lakesail.com").href, "https://examples.lakesail.com/");
  assert.equal(siteAddress("/sail-examples/", "https://lakehq.github.io").href, "https://lakehq.github.io/sail-examples/");
  for (const origin of ["http://example.com", "https://user:pass@example.com", "https://example.com/path/", "https://example.com/?x=1", "https://example.com/#hash", "file:///tmp/output"]) {
    assert.throws(() => siteAddress("/", origin));
  }
  for (const base of ["//example.com/", "/../", "/x/../../", "/x?y/", "relative/", "/missing-trailing-slash"]) {
    assert.throws(() => siteAddress(base));
  }
});

test("SVG placeholders escape page text and never evaluate nested placeholders", () => {
  const page = { title: 'A < B & "C" {{hostname}}', section: "Behavior", description: '<image href="file:///etc/passwd"/> & data' };
  const svg = previewSvg(template, "", page, "examples.lakesail.com");
  assert.ok(svg.includes("&lt;"));
  assert.ok(svg.includes("&amp;"));
  assert.ok(svg.includes("{{hostname}}"));
  assert.ok(!svg.includes('<image href="file:'));
  const renderer = new Resvg(svg, { font: { loadSystemFonts: false } });
  assert.deepEqual(renderer.imagesToResolve(), []);
});

test("title layout preserves long titles and rejects text that cannot fit", () => {
  const title = "Normalize nested order events without losing rejected records";
  const fit = fitText(title, { maxSize: 68, minSize: 40, width: 696, maxLines: 3, weight: 700 });
  assert.equal(fit.lines.join(" "), title);
  assert.ok(fit.lines.length <= 3);
  assert.throws(() => fitText("W".repeat(500), { maxSize: 68, minSize: 40, width: 696, maxLines: 3, weight: 700 }));
});

test("description excerpts fit even when a token is wider than the image", () => {
  const fit = fitText("W".repeat(150) + " more words", { maxSize: 26, minSize: 20, width: 696, maxLines: 3, truncate: true });
  assert.ok(fit.lines[0].endsWith("…"));
  assert.ok(fit.lines[0].length < 150);
});

test("generated JPEGs decode at the advertised dimensions", async () => {
  const folder = new URL("../_site/social/", import.meta.url);
  const files = await readdir(folder);
  assert.ok(files.length > 0);
  for (const file of files) {
    const data = await readFile(new URL(file, folder));
    const image = await sharp(data).metadata();
    assert.equal(image.format, "jpeg");
    assert.equal(image.width, 1200);
    assert.equal(image.height, 630);
  }
});
