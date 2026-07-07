import {
  smoke,
  botPrefetchGuard,
  backendApiDiagnostic,
  appHostApiDiagnostic,
  appHostPagesDiagnostic,
  diagnosticThresholds,
  handleSummary,
} from "./walnut_capacity_smoke.js";

const profile = (__ENV.TEST_PROFILE || "small").toLowerCase();

const profiles = {
  small: {
    vus: 25,
    stages: [
      { duration: "1m", target: 10 },
      { duration: "3m", target: 25 },
      { duration: "1m", target: 0 },
    ],
    coreP95Ms: 1000,
    overallP95Ms: 1500,
  },
  prod50: {
    vus: 50,
    stages: [
      { duration: "2m", target: 25 },
      { duration: "3m", target: 50 },
      { duration: "2m", target: 50 },
      { duration: "2m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  prod75: {
    vus: 75,
    stages: [
      { duration: "2m", target: 25 },
      { duration: "3m", target: 50 },
      { duration: "3m", target: 75 },
      { duration: "2m", target: 75 },
      { duration: "2m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  prod200: {
    vus: 200,
    stages: [
      { duration: "4m", target: 100 },
      { duration: "5m", target: 150 },
      { duration: "5m", target: 200 },
      { duration: "2m", target: 200 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  prod300: {
    vus: 300,
    stages: [
      { duration: "4m", target: 150 },
      { duration: "5m", target: 250 },
      { duration: "4m", target: 300 },
      { duration: "2m", target: 300 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  prod400: {
    vus: 400,
    stages: [
      { duration: "4m", target: 200 },
      { duration: "5m", target: 300 },
      { duration: "5m", target: 400 },
      { duration: "2m", target: 400 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  backend_api_400: {
    vus: 400,
    exec: "backendApiDiagnostic",
    botGuard: false,
    stages: [
      { duration: "4m", target: 200 },
      { duration: "5m", target: 300 },
      { duration: "5m", target: 400 },
      { duration: "2m", target: 400 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  apphost_api_400: {
    vus: 400,
    exec: "appHostApiDiagnostic",
    botGuard: false,
    stages: [
      { duration: "4m", target: 200 },
      { duration: "5m", target: 300 },
      { duration: "5m", target: 400 },
      { duration: "2m", target: 400 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  apphost_pages_400: {
    vus: 400,
    exec: "appHostPagesDiagnostic",
    botGuard: false,
    stages: [
      { duration: "4m", target: 200 },
      { duration: "5m", target: 300 },
      { duration: "5m", target: 400 },
      { duration: "2m", target: 400 },
      { duration: "4m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  apphost_pages_500: {
    vus: 500,
    exec: "appHostPagesDiagnostic",
    botGuard: false,
    stages: [
      { duration: "3m", target: 250 },
      { duration: "5m", target: 500 },
      { duration: "2m", target: 500 },
      { duration: "3m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  medium: {
    vus: 100,
    stages: [
      { duration: "2m", target: 50 },
      { duration: "5m", target: 100 },
      { duration: "2m", target: 0 },
    ],
    coreP95Ms: 1500,
    overallP95Ms: 2000,
  },
  large: {
    vus: 250,
    stages: [
      { duration: "3m", target: 100 },
      { duration: "8m", target: 250 },
      { duration: "3m", target: 0 },
    ],
    coreP95Ms: 2000,
    overallP95Ms: 2500,
  },
  target: {
    vus: 1000,
    stages: [
      { duration: "5m", target: 250 },
      { duration: "10m", target: 1000 },
      { duration: "5m", target: 0 },
    ],
    coreP95Ms: 2500,
    overallP95Ms: 3000,
  },
};

if (!profiles[profile]) {
  throw new Error(`Unknown TEST_PROFILE=${profile}. Use small, prod50, prod75, prod200, prod300, prod400, backend_api_400, apphost_api_400, apphost_pages_400, apphost_pages_500, medium, large, or target.`);
}

const selected = profiles[profile];

export const options = {
  scenarios: {
    capacity: {
      executor: "ramping-vus",
      stages: selected.stages,
      gracefulRampDown: "30s",
      exec: selected.exec || "smoke",
    },
    ...(selected.botGuard === false
      ? {}
      : {
          bot_prefetch_guard: {
            executor: "constant-vus",
            vus: 1,
            duration: "1m",
            exec: "botPrefetchGuard",
            startTime: "15s",
          },
        }),
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    "checks{route_priority:core}": ["rate>0.99"],
    [`http_req_duration{route_priority:core}`]: [`p(95)<${selected.coreP95Ms}`],
    http_req_duration: [`p(95)<${selected.overallP95Ms}`],
    five_xx_rate: ["rate<0.001"],
    ...diagnosticThresholds(),
  },
};

export { smoke, botPrefetchGuard, backendApiDiagnostic, appHostApiDiagnostic, appHostPagesDiagnostic, handleSummary };
