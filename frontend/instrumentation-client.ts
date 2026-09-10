import { ensureHeyCatch } from "./lib/heycatch";
import { privacyConsentChangedEvent } from "./lib/privacyConsent";

ensureHeyCatch();
window.addEventListener(privacyConsentChangedEvent, () => ensureHeyCatch());
