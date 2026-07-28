const DEFAULT_BACKEND = "https://congress-tracker-api.fly.dev";

const statusEl = document.querySelector("#status");
const draftsEl = document.querySelector("#drafts");
const backendInput = document.querySelector("#backendUrl");
const tokenInput = document.querySelector("#token");

document.querySelector("#saveToken").addEventListener("click", saveSettings);
document.querySelector("#refresh").addEventListener("click", loadDrafts);
document.querySelector("#openReddit").addEventListener("click", () => chrome.tabs.create({ url: "https://ads.reddit.com/" }));

init();

async function init() {
  const saved = await chrome.storage.session.get(["backendUrl", "extensionToken"]);
  backendInput.value = saved.backendUrl || DEFAULT_BACKEND;
  tokenInput.value = saved.extensionToken || "";
  await loadDrafts();
}

async function saveSettings() {
  await chrome.storage.session.set({
    backendUrl: backendInput.value.trim() || DEFAULT_BACKEND,
    extensionToken: tokenInput.value.trim(),
  });
  setStatus("Token saved for this browser session.");
}

async function loadDrafts() {
  await saveSettings();
  const { backendUrl, extensionToken } = await chrome.storage.session.get(["backendUrl", "extensionToken"]);
  if (!extensionToken) {
    setStatus("Paste a short-lived extension token from Walnut Admin.");
    renderDrafts([]);
    return;
  }
  try {
    const response = await fetch(`${backendUrl || DEFAULT_BACKEND}/api/extension/reddit-ads/drafts`, {
      headers: { Authorization: `Bearer ${extensionToken}` },
      credentials: "omit",
    });
    if (!response.ok) throw new Error(`Draft request failed (${response.status}).`);
    const payload = await response.json();
    renderDrafts(payload.items || []);
    setStatus(`${(payload.items || []).length} approved draft(s) available.`);
  } catch (error) {
    setStatus(error instanceof Error ? error.message : "Unable to load drafts.");
    renderDrafts([]);
  }
}

function renderDrafts(drafts) {
  draftsEl.textContent = "";
  drafts.forEach((draft) => {
    const fields = approvedFields(draft);
    const card = document.createElement("article");
    card.className = "draft";
    card.innerHTML = `
      <span class="badge">Approved</span>
      <h2></h2>
      <p></p>
      <div class="actions"></div>
    `;
    card.querySelector("h2").textContent = fields.headline;
    card.querySelector("p").textContent = fields.primaryText;
    const actions = card.querySelector(".actions");
    actions.append(
      button("Copy headline", () => copy(fields.headline)),
      button("Copy body", () => copy(fields.primaryText)),
      button("Copy URL", () => copy(fields.destinationUrl)),
      button("Copy CTA", () => copy(fields.cta)),
      button("Fill approved draft", () => sendFillMessage(draft), "secondary"),
    );
    draftsEl.append(card);
  });
}

function approvedFields(draft) {
  const finalDraft = draft.final_draft || {};
  return {
    id: draft.id,
    headline: finalDraft.headline || "",
    primaryText: finalDraft.primary_text || "",
    destinationUrl: finalDraft.destination_url || draft.destination_url || "",
    cta: finalDraft.cta || "",
    targetingNotes: [draft.audience, draft.geography, draft.campaign_objective].filter(Boolean).join(" | "),
  };
}

function button(label, onClick, className = "") {
  const element = document.createElement("button");
  element.type = "button";
  element.className = className;
  element.textContent = label;
  element.addEventListener("click", onClick);
  return element;
}

async function copy(text) {
  await navigator.clipboard.writeText(text || "");
  setStatus("Copied.");
}

async function sendFillMessage(draft) {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id || !/^https:\/\/ads\.reddit\.com\//.test(tab.url || "")) {
    await chrome.tabs.create({ url: "https://ads.reddit.com/" });
    setStatus("Opened Reddit Ads Manager. Click Fill again after the page loads.");
    return;
  }
  const response = await chrome.tabs.sendMessage(tab.id, { type: "WALNUT_FILL_APPROVED_DRAFT", draft });
  if (!response?.ok) setStatus(response?.message || "Reddit Ads Manager fields were not recognized.");
  else {
    await logFillAction(draft.id, response.fields || []);
    setStatus(`Prepared ${response.fields.length} field(s). Review before submitting.`);
  }
}

function setStatus(message) {
  statusEl.textContent = message;
}

async function logFillAction(draftId, fields) {
  const { backendUrl, extensionToken } = await chrome.storage.session.get(["backendUrl", "extensionToken"]);
  if (!extensionToken) return;
  await fetch(`${backendUrl || DEFAULT_BACKEND}/api/extension/reddit-ads/drafts/${draftId}/fill-action`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${extensionToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
    credentials: "omit",
  });
}
