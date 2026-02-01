# Congress Tracker Frontend

## Environment variables

Set the following environment variable in Vercel (or your local `.env.local`) so the frontend can reach the API:

| Name | Value |
| --- | --- |
| `NEXT_PUBLIC_API_BASE_URL` | `https://congress-tracker-api.fly.dev` |

In development, the app will fall back to `https://congress-tracker-api.fly.dev` if `NEXT_PUBLIC_API_BASE_URL` is not set.
