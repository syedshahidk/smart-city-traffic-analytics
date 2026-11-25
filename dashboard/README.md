# Flowstate Traffic Intelligence dashboard

A responsive React dashboard for the Smart City Traffic Analytics FastAPI service. It displays API-backed historical metrics, weighted KPIs, trends, paginated records, dataset information, and validated 15-minute forecasts. Historical benchmark data is replayed through a simulated stream; the dashboard does not represent actual live roadway traffic.

## Local development

Requirements: Node.js 22 LTS or later and npm.

1. Start the API using the repository's API Compose file. Configure its database credentials in the root `.env` as described by the root `.env.example`.

   ```powershell
   docker compose -f docker-compose.api.yml up --build -d api
   ```

2. In `dashboard/`, create `.env.local` with the browser-visible API URL:

   ```dotenv
   VITE_API_BASE_URL=http://127.0.0.1:8000
   ```

   This value is public frontend configuration. Never put database credentials or other secrets in a `VITE_` variable.

3. Install dependencies and start Vite:

   ```powershell
   cd dashboard
   npm ci
   npm run dev
   ```

   Open `http://localhost:5173`. The API allows GET requests from the two local Vite origins by default. To change those origins, set `API_CORS_ORIGINS` in the root `.env` to a comma-separated list.

4. Check types and create a production bundle:

   ```powershell
   npm run typecheck
   npm run build
   ```

## Docker dashboard

The dashboard has a standalone Compose file and does not join the Kafka/Spark/PostgreSQL/HDFS network. Its browser calls the API through the host-bound `127.0.0.1:8000` port.

```powershell
docker compose -f docker-compose.dashboard.yml up --build -d
```

Open `http://127.0.0.1:8080`. Set `VITE_API_BASE_URL` at image build time if the browser reaches the API at another address. Rebuild the dashboard image after changing that value.

## Dashboard features

- **Overview:** weighted speed and congestion KPIs, traffic trend, recent records, dataset metadata, and API readiness.
- **Traffic analytics:** separate speed and congestion charts with API-supported 5-minute, 15-minute, hourly, and daily buckets.
- **Forecasting:** latest API forecast, model/version details, input window and freshness. Stale or unavailable forecasts are shown as unavailable; no prediction is fabricated.
- **Data explorer:** paginated metric records with dataset and time filters.
- **About & sources:** dataset context and the Kafka → Spark → PostgreSQL/HDFS → ML/FastAPI pipeline.

The sensor network drawing is an illustrative schematic, not a geographic map; no reliable sensor coordinates are present in the repository.
