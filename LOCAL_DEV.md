# LOCAL_DEV.md

## Purpose

This project supports a fully local development workflow so you can build and test features without touching the live Vercel frontend, Fly backend, production database, or live Stripe environment.

The intended local setup is:

- **Frontend** on `http://localhost:3000`
- **Backend** on `http://localhost:8080`
- **Local database**
- **Stripe test mode**
- **Local auth / Google OAuth callback**
- **No production API calls unless you intentionally switch**

Once things work locally, you can commit and push.

---

## Local architecture

### Local frontend
- Runs on `localhost:3000`
- Points to the local backend
- Uses local/public test env vars

### Local backend
- Runs on `localhost:8080`
- Uses a local SQLite DB
- Uses test Stripe keys
- Uses local auth/OAuth settings

### Production safety rule
Your default dev workflow should **never** point at:
- `https://congress-tracker-api.fly.dev`
- production DB
- live Stripe keys

Only use production endpoints intentionally for comparison/debugging.

---

## Frontend environment

Create or update:

`frontend/.env.local`

```env
NEXT_PUBLIC_API_BASE=http://localhost:8080
NEXT_PUBLIC_API_BASE_URL=http://localhost:8080