# Twitch & Kick Drops Miner

A unified, headless Linux bot that combines the features of **[DevilXD/TwitchDropsMiner](https://github.com/DevilXD/TwitchDropsMiner)** and **[HyperBeats/KickDropsMiner](https://github.com/HyperBeats/KickDropsMiner)** into a single lightweight application with a real-time Web Dashboard.

Designed for 24/7 headless operation on **Pterodactyl**, Linux servers, VPS, Raspberry Pi, or Docker—**no desktop GUI, X11, or headless browser required**.

---

## Key Features

- **Dual-Platform Simultaneous Mining**: Mines both **Twitch** and **Kick** drops concurrently in a single process.
- **Stream-less Bandwidth-Saving Simulation**:
  - **Twitch**: Uses Twitch's Android GraphQL protocol + PubSub WebSockets + lightweight HLS segment `HEAD` requests every 20 seconds (zero video bandwidth).
  - **Kick**: Uses `curl_cffi` TLS/HTTP2 Chrome impersonation (bypassing Cloudflare WAF headlessly) + Kick's Viewer WebSocket (`wss://websockets.kick.com/viewer/v1/connect`) sending `channel_handshake`, `ping`, and `tracking.user.watch.livestream` events.
- **Auto-Claim**:
  - Automatically claims time-based drops on both Twitch and Kick as soon as they reach 100% or become claimable.
  - Optional automatic Twitch Channel Points bonus chest claiming.
- **Priority Queue & Manual Watch Queue**:
  - **Game Priority List**: Reorder games on the web UI to control mining order.
  - **Priority Modes**:
    - `Priority Only`: Only mine games in your priority list.
    - `Priority Then All`: Mine priority games first, then fall back to other active campaigns.
    - `Ending Soonest`: Prioritize campaigns whose end date is closest.
    - `Completable First`: Prioritize drops with the fewest remaining minutes.
  - **Manual Channel Queue (Kick & Twitch)**: Queue specific streamer channels with automatic fallback to alternate channels in the same campaign if your preferred streamer goes offline.
- **Smart Auto-Switch**:
  - Automatically switches channels when a streamer goes offline, changes game category, or when a higher-priority campaign/streamer goes live.
- **Real-Time Web Dashboard (Port `8920` default)**:
  - Live progress bars, active channel status, campaign explorer, priority queue manager, claim history, Discord webhook alerts, and live logs over WebSockets.

---

## Hosting on Pterodactyl (Recommended)

This repository includes a ready-to-import Pterodactyl Egg: **[`egg-twitch-kick-drops-miner.json`](./egg-twitch-kick-drops-miner.json)**.

1. Download **`egg-twitch-kick-drops-miner.json`**.
2. In your **Pterodactyl Admin Panel**, go to **Nests** -> select a Nest -> click **Import Egg**, and upload `egg-twitch-kick-drops-miner.json`.
3. Create a new Server using the **Twitch & Kick Drops Miner** egg:
   - **Primary Allocation Port**: `8920` (the bot automatically binds to `{{SERVER_PORT}}` / `8920`).
   - **Memory**: `256 MB` – `512 MB` RAM is plenty.
   - **Disk**: `1024 MB`+.
4. Start the server and open **`http://<your-node-ip>:8920`** in your browser!

---

## Other Linux Deployment Options

### Option 2: Native Linux Installation (with optional `systemd` service)

```bash
chmod +x install_linux.sh
./install_linux.sh
```

To run manually:
```bash
./run.sh --host 0.0.0.0 --port 8920
```

Then open **`http://<your-linux-ip>:8920`** in your browser.

---

### Option 3: Docker Compose

```bash
docker compose up -d --build
```

Data, credentials, and settings are persisted in `./data`. Open **`http://<your-linux-ip>:8920`** in your browser.

---

## Authenticating Your Accounts

All authentication is done directly from the **Web Dashboard**:

### 1. Twitch Login (OAuth2 Device Code)
1. Click **Login with Code** in the **Twitch Miner** card.
2. A verification code (e.g., `ABCD-EFGH`) and link to `https://www.twitch.tv/activate` will appear.
3. Authorize on any device—the bot automatically detects authorization and starts mining.

### 2. Kick Login (3 Methods Supported)
Click **Connect Account** in the **Kick Miner** card:
- **Smart-TV Device Code**: Generates a 6-digit code and link (`https://kick.com/tv/login`) that automatically receives a bearer token once authorized.
- **Session Token**: Copy your `session_token` cookie value from an existing browser session on `kick.com` (DevTools -> Application -> Cookies -> `https://kick.com` -> `session_token`) and paste it in.
- **`cookies.json` Import**: Directly compatible with Cookie-Editor JSON exports used by `HyperBeats/KickDropsMiner`.

---

## Project Structure

- `egg-twitch-kick-drops-miner.json` — Pterodactyl Panel Egg (`PTDL_v2`)
- `main.py` — Application entrypoint & CLI arguments (defaults to port `8920` or `SERVER_PORT`)
- `core/models.py` — Unified data models for campaigns, drops, channels, and state
- `core/config.py` — Persistent JSON configuration, credentials, and claims store
- `core/events.py` — EventBus for WebSocket live broadcasting and Discord notifications
- `core/twitch_miner.py` — Twitch OAuth2 device auth, GQL client, PubSub listener, HLS watch loop
- `core/kick_miner.py` — Kick TLS-impersonated API client, Viewer WebSocket loop, auto-claimer
- `core/orchestrator.py` — Coordinates Twitch & Kick engines and unified actions
- `web/server.py` — FastAPI REST + WebSocket server
- `web/static/index.html` — Single-page responsive Web UI
