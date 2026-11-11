import { readFileSync } from "node:fs";
import { defineConfig } from "vitepress";
import { prepareMetadata } from "./metadata.mjs";

const navigation = JSON.parse(readFileSync(new URL("../.generated/navigation.json", import.meta.url), "utf8"));
const base = process.env.SAIL_EXAMPLES_BASE || "/";
const metadata = await prepareMetadata(base);

export default defineConfig({
  lang: "en-US",
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: `${base}sail.svg` }],
    ["meta", { name: "theme-color", content: "#ffffff", media: "(prefers-color-scheme: light)" }],
    ["meta", { name: "theme-color", content: "#1b1b1f", media: "(prefers-color-scheme: dark)" }],
  ],
  sitemap: {
    hostname: metadata.address.href,
    transformItems: items => items.filter(item => item.url !== "404.html"),
  },
  transformPageData(page) {
    const key = page.relativePath.replace(/\.md$/, ".html");
    page.frontmatter.head = [...(page.frontmatter.head || []), ...(metadata.heads.get(key) || [])];
  },
  title: "Sail examples",
  description: "Runnable examples with Spark SQL and Python on Sail.",
  base,
  srcDir: ".generated",
  outDir: "../_site",
  appearance: true,
  lastUpdated: false,
  cleanUrls: false,
  markdown: { html: true },
  themeConfig: {
    logo: "/sail.svg",
    siteTitle: "Sail examples",
    nav: [
      { text: "Explore", link: "/index.html#examples" },
      { text: "Behavior", link: "/facts/index.html" },
      { text: "Sail docs", link: "https://docs.lakesail.com/sail/latest/" },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/lakehq/sail-examples" }],
    sidebar: {
      "/examples/": navigation.examples,
      "/facts/": navigation.facts,
    },
    search: {
      provider: "local",
      options: {
        _render(src, env, md) {
          md.render(src, env);
          if (env.frontmatter.showcase || env.relativePath === "404.md") return "";
          return readFileSync(env.path.replace(/\.md$/, ".content.html"), "utf8");
        },
      },
    },
    outline: [2, 3],
    docFooter: { prev: false, next: false },
  },
});
