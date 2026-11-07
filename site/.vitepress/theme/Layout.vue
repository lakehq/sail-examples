<script setup>
import DefaultTheme from "vitepress/theme-without-fonts";
import VPSwitchAppearance from "vitepress/dist/client/theme-default/components/VPSwitchAppearance.vue";
import { useData, onContentUpdated } from "vitepress";
import { onMounted, onUnmounted, shallowRef } from "vue";
import { initializePage } from "../../site.js";

const { frontmatter } = useData();
const appearanceTarget = shallowRef(null);
let cleanup = () => {};

function initialize() {
  cleanup();
  cleanup = initializePage();
  appearanceTarget.value = document.getElementById("showcase-appearance");
}
onContentUpdated(initialize);
onMounted(initialize);
onUnmounted(() => cleanup());
</script>

<template>
  <Content v-if="frontmatter.showcase" class="sail-showcase" />
  <div v-else class="sail-docs">
    <DefaultTheme.Layout />
  </div>
  <Teleport v-if="frontmatter.showcase && appearanceTarget" :to="appearanceTarget">
    <VPSwitchAppearance />
  </Teleport>
</template>
