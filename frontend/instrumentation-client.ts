import { analytics } from "@heycatch/sdk";

const projectKey = process.env.NEXT_PUBLIC_HEYCATCH_PROJECT_KEY;

if (projectKey) {
  try {
    analytics.init({
      projectKey,
      install: {
        framework: "nextjs",
        frameworkVersion: "15",
        agent: "codex",
      },
    });
  } catch {
    // Analytics must never prevent the app from loading.
  }
}
