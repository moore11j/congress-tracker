import { analytics } from "@heycatch/sdk";

const projectKey = process.env.NEXT_PUBLIC_HEYCATCH_PROJECT_KEY;

if (projectKey) {
  analytics.init({
    projectKey,
    tracingHosts: ["congress-tracker-api.fly.dev"],
    install: {
      framework: "nextjs",
      frameworkVersion: "15",
      agent: "codex",
    },
  });
}
