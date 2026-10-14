# DB Chat

Connect to an existing PostgreSQL or MySQL database, then ask questions in plain English.
The AI proposes SQL, **nothing runs until you click "Approve & run"**.

## Quick start

```bash
npm install
cp .env.example .env      # then put your NVIDIA_API_KEY in .env
npm start
```

Open http://localhost:3000, enter your DB credentials, and start chatting.

## How it works

1. **Connect**: credentials go to the backend, which opens a small connection pool. They live only in
   server memory (30-min idle timeout) and are never written to disk or logged.
2. **Analyze**: the backend reads the schema (tables, columns, PK/FK, approximate row counts) from
   `information_schema`.
3. **Chat**: your message + the schema go to the model via the NVIDIA API, which replies with an explanation and, if needed,
   one SQL statement. Only the schema is sent to the AI, **never your table data or query results**.
4. **Approve**: the SQL appears in an editable box. Execution happens only through `/api/execute`, which
   accepts a one-time proposal ID created by the chat endpoint, triggered by your click.
5. **Run**: read queries run inside a `READ ONLY` transaction (enforced by the database). Write statements
   are blocked unless you ticked "Allow write statements" when connecting, and they ask for a second confirmation.
   Results are capped at 200 rows (`MAX_ROWS`).

## Choosing a model

After connecting, the app fetches the models available to your NVIDIA key and shows them in a dropdown (top right), so retired models never break it. Optionally set `NVIDIA_MODEL` in `.env` as the preferred default. For a self-hosted NIM, set `NVIDIA_BASE_URL`.

## Safety tips

- Use a database user with the minimum privileges (ideally read-only) for the best protection.
- This is a starter project for local use. Before exposing it to a network, add HTTPS, real user
  authentication, rate limiting, and restrict which hosts can be connected to (SSRF risk).

## Files

- `server.js`: Express API, DB adapters, NVIDIA API call, approval gate
- `public/index.html`: single-page frontend (connect form, schema sidebar, chat, approval cards)
