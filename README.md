# linebot_mvp (clean)

## Required Render Environment Variables
- LINE_CHANNEL_SECRET
- LINE_CHANNEL_ACCESS_TOKEN
- OPENAI_API_KEY
- DATABASE_URL (Render Postgres internal URL is recommended)
- SHOP_ID (e.g. tokyo_01)

## Optional
- ADMIN_API_KEY (protect /api/* /dashboard /jobs/*)
- DB_SSL_MODE=auto|verify|disable
- DEBUG=1

## Dashboard
- /dashboard (defaults: events + refresh 30s)
- /api/hot?view=latest|events

## Follow-up job
- POST /jobs/followup (protected if ADMIN_API_KEY set)
- Set FOLLOWUP_ENABLED=1 to enable.
- Use dry_run=1 for safe testing.

Example curl:
curl -X POST "https://YOUR.onrender.com/jobs/followup?dry_run=1&limit=10" \
  -H "X-Admin-Key: YOUR_ADMIN_API_KEY"
