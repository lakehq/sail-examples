export function initializePage() {
  const controller = new AbortController();
  const { signal } = controller;
  const cards = [...document.querySelectorAll("[data-example]")];
  const search = document.querySelector("#example-search");
  const level = document.querySelector("#example-level");
  const topics = [...document.querySelectorAll("[data-topic]")];
  let selectedTopic = "";

  function filterExamples() {
    const terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
    let visible = 0;
    for (const card of cards) {
      const matches =
        terms.every((term) => card.dataset.search.includes(term)) &&
        (!level.value || card.dataset.level === level.value) &&
        (!selectedTopic || card.dataset.category === selectedTopic);
      card.hidden = !matches;
      if (matches) visible += 1;
    }
    document.querySelector("#result-count").textContent = `${visible} ${visible === 1 ? "example" : "examples"}`;
    document.querySelector("#no-results").hidden = visible !== 0;
    for (const topic of topics) {
      topic.setAttribute("aria-pressed", String(topic.dataset.topic === selectedTopic));
    }
  }

  if (search) {
    document.querySelector(".catalog-controls").hidden = false;
    document.querySelector(".category-filters").hidden = false;
    search.addEventListener("input", filterExamples, { signal });
    level.addEventListener("change", filterExamples, { signal });
    for (const topic of topics) {
      topic.addEventListener(
        "click",
        () => {
          selectedTopic = topic.dataset.topic;
          filterExamples();
        },
        { signal },
      );
    }
    document.querySelector("#clear-filters").addEventListener(
      "click",
      () => {
        search.value = "";
        level.value = "";
        selectedTopic = "";
        filterExamples();
        search.focus();
      },
      { signal },
    );
  }

  for (const button of document.querySelectorAll("[data-copy]")) {
    button.hidden = false;
    button.addEventListener(
      "click",
      async () => {
        const source = document.getElementById(button.dataset.copy);
        const status = button.closest("[data-copy-container], .terminal").querySelector(".copy-status");
        const label = button.dataset.copyLabel || "Commands";
        try {
          await navigator.clipboard.writeText(source.textContent);
          status.textContent = `${label} copied.`;
        } catch {
          const selection = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(source);
          selection.removeAllRanges();
          selection.addRange(range);
          status.textContent = `${label} selected. Use your browser’s Copy command to copy them.`;
        }
      },
      { signal },
    );
  }

  return () => controller.abort();
}
