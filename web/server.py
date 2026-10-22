from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from core.models import PriorityMode
from core.orchestrator import MinerOrchestrator

STATIC_DIR = Path(__file__).resolve().parent / "static"
orchestrator = MinerOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await orchestrator.start()
    try:
        yield
    finally:
        await orchestrator.stop()


app = FastAPI(
    title="Twitch & Kick Drops Miner",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- Request Schemas ---
class TokenLoginRequest(BaseModel):
    token: str


class KickCredentialsRequest(BaseModel):
    session_token: Optional[str] = None
    cookies_json: Optional[str] = None


class PriorityUpdateRequest(BaseModel):
    priority_mode: Optional[str] = None
    twitch_priority_only: Optional[bool] = None
    kick_priority_only: Optional[bool] = None
    twitch_priority_games: Optional[list[str]] = None
    kick_priority_games: Optional[list[str]] = None
    excluded_games: Optional[list[str]] = None


class PriorityGameActionRequest(BaseModel):
    platform: str  # "twitch" | "kick" | "exclude"
    game_name: str
    action: str    # "add" | "remove" | "up" | "down" | "top"


class QueueAddRequest(BaseModel):
    url: str
    target_minutes: int = 120
    game_name: str = ""


class QueueReorderRequest(BaseModel):
    item_ids: list[str]


class ClaimRequest(BaseModel):
    platform: str
    campaign_id: Optional[str] = None
    drop_id: Optional[str] = None


class QueueCampaignRequest(BaseModel):
    platform: str
    campaign_id: str


class SwitchChannelRequest(BaseModel):
    platform: str
    username: Optional[str] = None


class SettingsUpdateRequest(BaseModel):
    twitch_enabled: Optional[bool] = None
    kick_enabled: Optional[bool] = None
    auto_claim: Optional[bool] = None
    auto_switch: Optional[bool] = None
    auto_claim_points: Optional[bool] = None
    enable_badges_emotes: Optional[bool] = None
    prefer_manual_queue: Optional[bool] = None
    priority_mode: Optional[str] = None
    twitch_priority_only: Optional[bool] = None
    kick_priority_only: Optional[bool] = None
    inventory_refresh_minutes: Optional[int] = None
    channel_check_seconds: Optional[int] = None
    proxy: Optional[str] = None
    discord_webhook_url: Optional[str] = None


# --- Routes ---
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    data = orchestrator.get_full_status()
    data["available_games"] = orchestrator.get_available_games()
    return data


@app.get("/api/campaigns")
async def get_campaigns(platform: str = "all") -> dict[str, Any]:
    return {
        "campaigns": orchestrator.get_all_campaigns(platform),
        "available_games": orchestrator.get_available_games(),
    }


@app.post("/api/campaigns/refresh")
async def refresh_campaigns(platform: str = "all") -> dict[str, Any]:
    tasks = []
    if platform in ("all", "twitch"):
        tasks.append(orchestrator.twitch.fetch_inventory())
    if platform in ("all", "kick"):
        tasks.append(orchestrator.kick.fetch_campaigns_and_progress())
    await asyncio.gather(*tasks, return_exceptions=True)
    return {
        "ok": True,
        "campaigns": orchestrator.get_all_campaigns(platform),
    }


@app.post("/api/campaigns/claim")
async def claim_drops(req: ClaimRequest) -> dict[str, Any]:
    claimed = 0
    if req.platform in ("all", "twitch"):
        if req.drop_id:
            drop = orchestrator.twitch._drops_by_id.get(req.drop_id)
            if drop and await orchestrator.twitch.claim_drop(drop):
                claimed += 1
        else:
            claimed += await orchestrator.twitch.claim_all_ready()

    if req.platform in ("all", "kick"):
        if req.campaign_id and req.drop_id:
            camp = orchestrator.kick._campaigns_by_id.get(req.campaign_id)
            if camp:
                drop = next((d for d in camp.drops if d.id == req.drop_id), None)
                if drop and await orchestrator.kick.claim_drop(camp, drop):
                    claimed += 1
        else:
            claimed += await orchestrator.kick.claim_all_ready()

    return {"ok": True, "claimed": claimed}


@app.post("/api/campaigns/queue")
async def queue_campaign(req: QueueCampaignRequest) -> dict[str, Any]:
    try:
        return await orchestrator.queue_campaign(req.platform, req.campaign_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/priority")
async def update_priority(req: PriorityUpdateRequest) -> dict[str, Any]:
    cfg = orchestrator.config
    if req.priority_mode is not None:
        try:
            cfg.priority_mode = PriorityMode(req.priority_mode)
        except ValueError:
            pass
    if req.twitch_priority_only is not None:
        cfg.twitch_priority_only = req.twitch_priority_only
    if req.kick_priority_only is not None:
        cfg.kick_priority_only = req.kick_priority_only
    if req.twitch_priority_games is not None:
        cfg.twitch_priority_games = [g.strip() for g in req.twitch_priority_games if g.strip()]
    if req.kick_priority_games is not None:
        cfg.kick_priority_games = [g.strip() for g in req.kick_priority_games if g.strip()]
    if req.excluded_games is not None:
        cfg.excluded_games = [g.strip() for g in req.excluded_games if g.strip()]
    cfg.save()
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {"ok": True, "config": cfg.to_dict()}


@app.post("/api/priority/game")
async def modify_priority_game(req: PriorityGameActionRequest) -> dict[str, Any]:
    cfg = orchestrator.config
    game = req.game_name.strip()
    if not game:
        raise HTTPException(status_code=400, detail="Game name required")

    if req.platform == "twitch":
        target = cfg.twitch_priority_games
    elif req.platform == "kick":
        target = cfg.kick_priority_games
    elif req.platform == "exclude":
        target = cfg.excluded_games
    else:
        raise HTTPException(status_code=400, detail="Invalid target list")

    existing_match = next((g for g in target if g.strip().lower() == game.lower()), None)

    if req.action == "add":
        if not existing_match:
            target.append(game)
        if req.platform == "exclude":
            cfg.manual_queue = [
                q
                for q in cfg.manual_queue
                if (q.game_name or "").strip().lower() != game.lower()
            ]
    elif req.action == "remove":
        if existing_match:
            target.remove(existing_match)
        if req.platform in ("twitch", "kick"):
            cfg.manual_queue = [
                q
                for q in cfg.manual_queue
                if not (
                    q.platform.value == req.platform
                    and q.campaign_id
                    and (q.game_name or "").strip().lower() == game.lower()
                )
            ]
    elif req.action == "top":
        if existing_match:
            target.remove(existing_match)
        target.insert(0, existing_match or game)
    elif req.action == "up" and existing_match:
        idx = target.index(existing_match)
        if idx > 0:
            target[idx - 1], target[idx] = target[idx], target[idx - 1]
    elif req.action == "down" and existing_match:
        idx = target.index(existing_match)
        if idx < len(target) - 1:
            target[idx + 1], target[idx] = target[idx], target[idx + 1]

    cfg.save()
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {"ok": True, "config": cfg.to_dict()}


@app.get("/api/queue")
async def get_manual_queue() -> dict[str, Any]:
    return {
        "queue": [item.model_dump(mode="json") for item in orchestrator.config.manual_queue]
    }


@app.post("/api/queue/add")
async def add_manual_queue(req: QueueAddRequest) -> dict[str, Any]:
    item = orchestrator.config.add_manual_queue_item(
        url=req.url,
        target_minutes=req.target_minutes,
        game_name=req.game_name,
    )
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {
        "ok": True,
        "item": item.model_dump(mode="json"),
        "queue": [i.model_dump(mode="json") for i in orchestrator.config.manual_queue],
    }


@app.delete("/api/queue/{item_id}")
async def delete_manual_queue_item(item_id: str) -> dict[str, Any]:
    cfg = orchestrator.config
    cfg.manual_queue = [i for i in cfg.manual_queue if i.id != item_id]
    cfg.save()
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {
        "ok": True,
        "queue": [i.model_dump(mode="json") for i in cfg.manual_queue],
    }


@app.post("/api/queue/{item_id}/reset")
async def reset_manual_queue_item(item_id: str) -> dict[str, Any]:
    cfg = orchestrator.config
    for item in cfg.manual_queue:
        if item.id == item_id:
            item.finished = False
            item.elapsed_seconds = 0
    cfg.save()
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {
        "ok": True,
        "queue": [i.model_dump(mode="json") for i in cfg.manual_queue],
    }


@app.post("/api/queue/reorder")
async def reorder_manual_queue(req: QueueReorderRequest) -> dict[str, Any]:
    cfg = orchestrator.config
    by_id = {i.id: i for i in cfg.manual_queue}
    new_list = [by_id[iid] for iid in req.item_ids if iid in by_id]
    for remaining in cfg.manual_queue:
        if remaining.id not in req.item_ids:
            new_list.append(remaining)
    cfg.manual_queue = new_list
    cfg.save()
    return {
        "ok": True,
        "queue": [i.model_dump(mode="json") for i in cfg.manual_queue],
    }


@app.post("/api/settings")
async def update_settings(req: SettingsUpdateRequest) -> dict[str, Any]:
    cfg = orchestrator.config
    if req.twitch_enabled is not None:
        cfg.twitch_enabled = req.twitch_enabled
    if req.kick_enabled is not None:
        cfg.kick_enabled = req.kick_enabled
    if req.auto_claim is not None:
        cfg.auto_claim = req.auto_claim
    if req.auto_switch is not None:
        cfg.auto_switch = req.auto_switch
    if req.auto_claim_points is not None:
        cfg.auto_claim_points = req.auto_claim_points
    if req.enable_badges_emotes is not None:
        cfg.enable_badges_emotes = req.enable_badges_emotes
    if req.prefer_manual_queue is not None:
        cfg.prefer_manual_queue = req.prefer_manual_queue
    if req.priority_mode is not None:
        try:
            cfg.priority_mode = PriorityMode(req.priority_mode)
        except ValueError:
            pass
    if req.twitch_priority_only is not None:
        cfg.twitch_priority_only = req.twitch_priority_only
    if req.kick_priority_only is not None:
        cfg.kick_priority_only = req.kick_priority_only
    if req.inventory_refresh_minutes is not None:
        cfg.inventory_refresh_minutes = max(5, req.inventory_refresh_minutes)
    if req.channel_check_seconds is not None:
        cfg.channel_check_seconds = max(15, req.channel_check_seconds)
    if req.proxy is not None:
        cfg.proxy = req.proxy.strip()
    if req.discord_webhook_url is not None:
        cfg.discord_webhook_url = req.discord_webhook_url.strip()

    cfg.save()
    orchestrator.twitch._emit_state()
    orchestrator.kick._emit_state()
    orchestrator.twitch.wake()
    orchestrator.kick.wake()
    return {"ok": True, "config": cfg.to_dict()}


@app.post("/api/switch")
async def switch_channel(req: SwitchChannelRequest) -> dict[str, Any]:
    if req.platform == "twitch":
        if req.username:
            ok = await orchestrator.twitch.switch_to_channel(req.username)
            if not ok:
                raise HTTPException(status_code=400, detail=f"Twitch channel {req.username} is offline or invalid")
        else:
            orchestrator.twitch.state.watching_channel = None
            orchestrator.twitch._last_channel_check = 0.0
            orchestrator.twitch.wake()
    elif req.platform == "kick":
        if req.username:
            ok = await orchestrator.kick.switch_to_channel(req.username)
            if not ok:
                raise HTTPException(status_code=400, detail=f"Kick channel {req.username} is offline or invalid")
        else:
            orchestrator.kick.state.watching_channel = None
            orchestrator.kick.wake()
    return {"ok": True}


# --- Authentication Endpoints ---
@app.post("/api/auth/twitch/device")
async def twitch_device_auth() -> dict[str, Any]:
    try:
        flow = await orchestrator.twitch.start_device_login()
        return {"ok": True, "flow": flow}
    except Exception as e:
        orchestrator.events.log(f"Twitch device login error: {e}", platform="twitch", level="error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/twitch/token")
async def twitch_token_auth(req: TokenLoginRequest) -> dict[str, Any]:
    ok = await orchestrator.twitch.set_manual_token(req.token)
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid Twitch access token")
    return {"ok": True, "state": orchestrator.twitch.state.model_dump(mode="json")}


@app.post("/api/auth/twitch/logout")
async def twitch_logout() -> dict[str, Any]:
    await orchestrator.twitch.logout()
    return {"ok": True}


@app.post("/api/auth/kick/tv")
async def kick_tv_auth() -> dict[str, Any]:
    try:
        flow = await orchestrator.kick.start_tv_device_login()
        return {"ok": True, "flow": flow}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auth/kick/credentials")
async def kick_credentials_auth(req: KickCredentialsRequest) -> dict[str, Any]:
    ok = await orchestrator.kick.set_manual_credentials(
        session_token=req.session_token,
        cookies_json=req.cookies_json,
    )
    if not ok:
        raise HTTPException(
            status_code=401,
            detail="Could not verify Kick credentials. Check your session_token or cookies JSON.",
        )
    return {"ok": True, "state": orchestrator.kick.state.model_dump(mode="json")}


@app.post("/api/auth/kick/logout")
async def kick_logout() -> dict[str, Any]:
    await orchestrator.kick.logout()
    return {"ok": True}


# --- WebSocket for Live Updates ---
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    queue = orchestrator.events.subscribe()
    try:
        # Send initial snapshot
        await ws.send_json(
            {
                "type": "init",
                "data": orchestrator.get_full_status(),
            }
        )
        while True:
            event = await queue.get()
            await ws.send_json(event)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        orchestrator.events.unsubscribe(queue)


def create_app(data_dir: Optional[Path] = None) -> FastAPI:
    """Return the configured FastAPI application."""
    return app

