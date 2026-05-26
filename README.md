# PMF Lead Database

Persistent backend for tracking and deduplicating leads across batches.
Built for Premium Merchant Funding internal use.

## Stack

- Node.js + Express
- PostgreSQL (recommend Neon for the free-forever tier)
- Deployed on Render

## Local development

1. `npm install`
2. Copy `.env.example` to `.env` and fill in values
3. `npm start`
4. Test: http://localhost:3000/health

## Production deployment

### 1. Set up Neon (PostgreSQL)

1. Sign up at https://neon.tech (free, no card required)
2. Create a project
3. Copy the connection string (looks like `postgresql://...`)

### 2. Push to GitHub

```bash
git init
git add .
git commit -m "initial commit"
```

Then create a new repo on github.com and push.

### 3. Deploy on Render

1. Go to https://render.com -> New -> Web Service
2. Connect your GitHub repo
3. Configure:
   - Runtime: Node
   - Build command: `npm install`
   - Start command: `npm start`
4. Add environment variables:
   - `DATABASE_URL` (paste from Neon)
   - `AUTH_TOKEN` (pick a long random string; used by the frontend)
   - `NODE_ENV` = `production`
5. Deploy.

### 4. Verify

Once deployed, hit `https://YOUR-APP.onrender.com/health` in a browser. Should return `{"ok":true,...}`.

## API endpoints

All routes (except `/health`) require the `x-auth-token` header.

| Method | Path | Description |
|--------|------|-------------|
| GET    | `/health`              | Uptime check (no auth)        |
| GET    | `/api/stats`           | Database stats                |
| GET    | `/api/batches`         | List all batches              |
| POST   | `/api/batches`         | Upload a new batch            |
| DELETE | `/api/batches/:id`     | Delete a batch and its leads  |
| POST   | `/api/clear`           | Wipe entire database          |
| GET    | `/api/duplicates`      | Run dedup, return all groups  |

## Notes

- Free Render web service sleeps after 15 min of inactivity. First request after sleep takes ~30 sec to wake.
- Free Neon compute auto-suspends after inactivity (similar behavior).
- For always-on, upgrade Render to $7/mo.
