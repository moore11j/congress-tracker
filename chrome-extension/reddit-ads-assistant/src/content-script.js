const SELECTOR_VERSION = "reddit_ads_manager_2026_07_v1";
const FIELD_SELECTORS = {
  headline: [
    'input[name="headline"]',
    'textarea[name="headline"]',
    '[aria-label*="Headline" i]',
  ],
  primaryText: [
    'textarea[name="body"]',
    'textarea[name="text"]',
    '[aria-label*="Body" i]',
    '[aria-label*="Primary" i]',
  ],
  destinationUrl: [
    'input[name="destinationUrl"]',
    'input[name="url"]',
    '[aria-label*="Destination" i]',
    '[aria-label*="URL" i]',
  ],
  cta: [
    'input[name="callToAction"]',
    '[aria-label*="CTA" i]',
    '[aria-label*="Call to action" i]',
  ],
};

let undoSnapshot = [];

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type !== "WALNUT_FILL_APPROVED_DRAFT") return false;
  try {
    const result = showFillPanel(message.draft);
    sendResponse(result);
  } catch (error) {
    sendResponse({ ok: false, message: error instanceof Error ? error.message : "Unable to prepare fill panel." });
  }
  return true;
});

function showFillPanel(draft) {
  const fields = draftToFields(draft);
  const matches = recognizedFields(fields);
  if (!matches.length) {
    return { ok: false, message: "Reddit Ads Manager fields were not recognized. Use copy-to-clipboard instead.", selectorVersion: SELECTOR_VERSION };
  }
  document.querySelector("#walnut-reddit-ads-assistant-panel")?.remove();
  const panel = document.createElement("aside");
  panel.id = "walnut-reddit-ads-assistant-panel";
  panel.style.cssText = [
    "position:fixed",
    "right:16px",
    "top:16px",
    "z-index:2147483647",
    "width:330px",
    "background:#0f172a",
    "color:#e5e7eb",
    "border:1px solid rgba(255,255,255,.16)",
    "border-radius:8px",
    "box-shadow:0 20px 60px rgba(0,0,0,.35)",
    "font:13px system-ui,sans-serif",
    "padding:12px",
  ].join(";");
  panel.innerHTML = `
    <strong>Walnut approved draft</strong>
    <p style="color:#cbd5e1;line-height:1.4">Fields ready: ${matches.map((item) => item.key).join(", ")}. Verify all campaign settings before submission.</p>
    <button id="walnut-fill" type="button">Fill approved Walnut draft</button>
    <button id="walnut-undo" type="button">Undo</button>
    <button id="walnut-close" type="button">Close</button>
    <p style="color:#fcd34d;line-height:1.4">This assistant never submits, launches, changes payment, or sets budget automatically.</p>
  `;
  document.body.append(panel);
  panel.querySelectorAll("button").forEach((button) => {
    button.style.cssText = "margin:4px 4px 0 0;border:1px solid rgba(255,255,255,.2);border-radius:6px;padding:7px 9px;background:#10b981;color:#03111f;font-weight:700;cursor:pointer";
  });
  panel.querySelector("#walnut-fill").addEventListener("click", () => fillFields(matches));
  panel.querySelector("#walnut-undo").addEventListener("click", undoFill);
  panel.querySelector("#walnut-close").addEventListener("click", () => panel.remove());
  return { ok: true, fields: matches.map((item) => item.key), selectorVersion: SELECTOR_VERSION };
}

function draftToFields(draft) {
  const finalDraft = draft?.final_draft || {};
  return {
    headline: finalDraft.headline || "",
    primaryText: finalDraft.primary_text || "",
    destinationUrl: finalDraft.destination_url || draft?.destination_url || "",
    cta: finalDraft.cta || "",
  };
}

function recognizedFields(fields) {
  return Object.entries(fields)
    .map(([key, value]) => {
      const element = firstMatchingElement(FIELD_SELECTORS[key] || []);
      return element && value ? { key, value, element } : null;
    })
    .filter(Boolean);
}

function firstMatchingElement(selectors) {
  for (const selector of selectors) {
    const element = document.querySelector(selector);
    if (element && isEditable(element)) return element;
  }
  return null;
}

function isEditable(element) {
  if (!(element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement || element.isContentEditable)) return false;
  const type = element.getAttribute("type");
  return !element.disabled && !element.readOnly && type !== "hidden" && type !== "submit" && type !== "button";
}

function fillFields(matches) {
  undoSnapshot = matches.map(({ key, element }) => ({ key, element, value: readValue(element) }));
  for (const { element, value } of matches) {
    writeValue(element, value);
  }
}

function undoFill() {
  for (const { element, value } of undoSnapshot) {
    writeValue(element, value);
  }
}

function readValue(element) {
  return element.isContentEditable ? element.textContent || "" : element.value || "";
}

function writeValue(element, value) {
  if (element.isContentEditable) {
    element.textContent = value;
  } else {
    element.value = value;
  }
  element.dispatchEvent(new Event("input", { bubbles: true }));
  element.dispatchEvent(new Event("change", { bubbles: true }));
}

if (typeof module !== "undefined") {
  module.exports = { FIELD_SELECTORS, SELECTOR_VERSION, draftToFields, recognizedFields };
}
