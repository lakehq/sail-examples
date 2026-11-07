import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Resvg } from "@resvg/resvg-js";
import sharp from "sharp";

const source = new URL("../", import.meta.url);
const generated = new URL(".generated/", source);
const publicDirectory = new URL("public/", generated);
const font = {
  loadSystemFonts: false,
  fontFiles: [400, 700].map(weight => fileURLToPath(new URL(`fonts/outfit-${weight}.ttf`, source))),
  defaultFontFamily: "Outfit",
};

export function siteAddress(base, origin = process.env.SAIL_EXAMPLES_ORIGIN || "https://examples.lakesail.com") {
  const address = new URL(origin);
  if (address.protocol !== "https:" || address.username || address.password || address.port ||
      address.pathname !== "/" || address.search || address.hash) {
    throw new Error("SAIL_EXAMPLES_ORIGIN must be an HTTPS origin without credentials, port, path, query, or fragment");
  }
  if (!/^\/(?:[a-zA-Z0-9_-]+\/)*$/.test(base)) {
    throw new Error("SAIL_EXAMPLES_BASE must be / or a path of directory names ending in /");
  }
  return new URL(base, address);
}

export function xml(text) {
  return String(text).replace(/[&<>"']/g, character => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;",
  })[character]);
}

function textWidth(text, size, weight) {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="200"><text x="0" y="100" font-family="Outfit" font-size="${size}" font-weight="${weight}">${xml(text)}</text></svg>`;
  const renderer = new Resvg(svg, { font });
  return renderer.getBBox()?.width || 0;
}

export function fitText(text, { maxSize, minSize, width, maxLines, weight = 400, truncate = false }) {
  const words = text.trim().split(/\s+/);
  for (let size = maxSize; size >= minSize; size -= 2) {
    const lines = [];
    let line = "";
    for (const word of words) {
      const candidate = line ? `${line} ${word}` : word;
      if (line && textWidth(candidate, size, weight) > width) {
        lines.push(line);
        line = word;
      } else {
        line = candidate;
      }
    }
    if (line) lines.push(line);
    if (lines.length <= maxLines && lines.every(value => textWidth(value, size, weight) <= width)) {
      return { lines, size };
    }
    if (truncate && size === minSize) {
      const visible = lines.slice(0, maxLines);
      const shortened = visible.map((value, index) => {
        if (index < visible.length - 1 && textWidth(value, size, weight) <= width) return value;
        while (value && textWidth(`${value}…`, size, weight) > width) {
          const boundary = value.lastIndexOf(" ");
          value = value.slice(0, boundary > 0 ? boundary : -1);
        }
        return `${value.trimEnd()}…`;
      });
      return { lines: shortened, size };
    }
  }
  throw new Error(`Social preview text does not fit; shorten the page metadata: ${text}`);
}

function spans(lines, y, leading) {
  return lines.map((line, index) => `<tspan x="72" y="${y + index * leading}">${xml(line)}</tspan>`).join("");
}

export function previewDescription(text) {
  const plain = text.replace(/`([^`]+)`/g, "$1").replace(/\s+/g, " ").trim();
  if (plain.length <= 180) return plain;
  const sentences = [...new Intl.Segmenter("en", { granularity: "sentence" }).segment(plain)];
  const first = sentences[0].segment.trim();
  if (first.length <= 180) return first;
  const end = plain.lastIndexOf(" ", 179);
  return `${plain.slice(0, end > 0 ? end : 179)}…`;
}

export function previewSvg(template, logo, page, hostname) {
  const title = fitText(page.title, { maxSize: 68, minSize: 40, width: 696, maxLines: 3, weight: 700 });
  const description = fitText(previewDescription(page.description), { maxSize: 26, minSize: 20, width: 696, maxLines: 3, truncate: true });
  const section = fitText(page.section, { maxSize: 20, minSize: 20, width: 696, maxLines: 1 });
  const values = {
    logo,
    whiteLogo: logo.replace(/fill="#[a-fA-F0-9]+"/g, 'fill="#fff"'),
    title: spans(title.lines, 246, title.size * 1.08),
    titleSize: title.size,
    description: spans(description.lines, 440, description.size * 1.2),
    descriptionSize: description.size,
    section: xml(section.lines[0]),
    hostname: xml(hostname),
  };
  return template.replace(/\{\{(\w+)\}\}/g, (_, key) => {
    if (!(key in values)) throw new Error(`Unknown social template field: ${key}`);
    return values[key];
  });
}

function pageUrl(path, address) {
  return new URL(path.replace(/index\.html$/, ""), address).href;
}

async function writeNotices() {
  const notices = ["Third-party assets used by the Sail examples website\n"];
  for (const name of ["inter", "outfit", "jetbrains-mono"]) {
    const license = new URL(`../node_modules/@fontsource-variable/${name}/LICENSE`, source);
    notices.push(`${name}\n${await readFile(license, "utf8")}`);
  }
  const icons = await readFile(new URL("icons.svg", source), "utf8");
  const octicons = icons.match(/<!-- (GitHub mark:[\s\S]*?)-->/)?.[1];
  if (!octicons) throw new Error("The GitHub icon license notice is missing");
  notices.push(octicons.trim());
  await writeFile(new URL("third-party-notices.txt", publicDirectory), notices.join("\n\n"));
}

export async function prepareMetadata(base) {
  const address = siteAddress(base);
  const pages = JSON.parse(await readFile(new URL("pages.json", generated), "utf8"));
  const template = await readFile(new URL("social-preview.svg", source), "utf8");
  const logoSvg = await readFile(new URL("sail.svg", source), "utf8");
  const logo = logoSvg.replace(/<svg[^>]*>|<\/svg>/g, "").trim();
  await mkdir(new URL("social/", publicDirectory), { recursive: true });
  const heads = new Map();
  for (const page of pages) {
    if (!/^(?:[a-z0-9-]+\/)*[a-z0-9-]+\.html$/.test(page.url)) {
      throw new Error(`Invalid generated page path: ${page.url}`);
    }
    if (page.url === "404.html") {
      heads.set(page.url, [["meta", { name: "robots", content: "noindex" }]]);
      continue;
    }
    const svg = previewSvg(template, logo, page, address.host);
    const renderer = new Resvg(svg, { font });
    const pixels = renderer.render().pixels;
    const jpeg = await sharp(pixels, { raw: { width: 1200, height: 630, channels: 4 } })
      .jpeg({ quality: 90, chromaSubsampling: "4:4:4" }).toBuffer();
    const hash = createHash("sha256").update(jpeg).digest("hex").slice(0, 16);
    const imagePath = `social/${page.url.replace(/\.html$/, "").replaceAll("/", "-")}.${hash}.jpg`;
    await writeFile(new URL(imagePath, publicDirectory), jpeg);
    const canonical = pageUrl(page.url, address);
    const image = new URL(imagePath, address).href;
    const title = `${page.title} | Sail examples`;
    const alt = `${page.title}. ${page.section}. Sail examples.`;
    heads.set(page.url, [
      ["link", { rel: "canonical", href: canonical }],
      ...Object.entries({
        "og:type": "website", "og:site_name": "Sail examples", "og:locale": "en_US",
        "og:title": title, "og:description": page.description, "og:url": canonical,
        "og:image": image, "og:image:type": "image/jpeg", "og:image:width": "1200",
        "og:image:height": "630", "og:image:alt": alt,
      }).map(([property, content]) => ["meta", { property, content }]),
      ...Object.entries({
        "twitter:card": "summary_large_image", "twitter:title": title,
        "twitter:description": page.description, "twitter:image": image, "twitter:image:alt": alt,
      }).map(([name, content]) => ["meta", { name, content }]),
    ]);
  }
  await writeFile(new URL("robots.txt", publicDirectory), `User-agent: *\nAllow: /\n\nSitemap: ${new URL("sitemap.xml", address).href}\n`);
  await writeNotices();
  return { address, heads };
}
