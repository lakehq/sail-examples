import DefaultTheme from "vitepress/theme-without-fonts";
import "@fontsource-variable/inter";
import "@fontsource-variable/outfit";
import "@fontsource-variable/jetbrains-mono";
import "./custom.css";
import "../../site.css";
import Layout from "./Layout.vue";

export default { extends: DefaultTheme, Layout };
