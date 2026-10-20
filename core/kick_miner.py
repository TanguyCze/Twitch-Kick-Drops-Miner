from __future__ import annotations

import asyncio
import json
import random
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import websockets

try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

import aiohttp

from .config import AppConfig
from .events import EventBus
from .models import (
    ChannelInfo,
    DropCampaign,
    DropReward,
    ManualQueueItem,
    MinerStatus,
    Platform,
    PlatformMinerState,
    PriorityMode,
)

KICK_CLIENT_TOKEN = "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823"
KICK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
KICK_ASSET_BASE = "https://ext.kick.com"


def parse_kick_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _numeric_or_str(val: str | int) -> int | str:
    s = str(val).strip()
    if s.isdigit():
        return int(s)
    return s


class KickMiner:
    """
    Headless Kick Drops Miner combining HyperBeats/KickDropsMiner queue & campaign logic
    with stream-less Viewer WebSocket heartbeats and curl_cffi Cloudflare WAF impersonation.
    """

    def __init__(self, config: AppConfig, events: EventBus) -> None:
        self.config = config
        self.events = events
        self.state = PlatformMinerState(
            platform=Platform.KICK,
            enabled=config.kick_enabled,
            watch_method="Viewer WebSocket (Stream-less)",
        )
        self.campaigns: list[DropCampaign] = []
        self._campaigns_by_id: dict[str, DropCampaign] = {}
        self._curl_session: Optional[Any] = None
        self._aio_session: Optional[aiohttp.ClientSession] = None

        self._running = False
        self._main_task: Optional[asyncio.Task[None]] = None
        self._ws_watch_task: Optional[asyncio.Task[None]] = None
        self._auth_task: Optional[asyncio.Task[None]] = None
        self._wake_event = asyncio.Event()

        self._last_inventory_fetch: float = 0.0
        self._last_progress_fetch: float = 0.0
        self._last_channel_check: float = 0.0
        self._watch_interval_sec: int = 120
        self._suppressed_claims: set[str] = set()

    def _emit_state(self) -> None:
        self.state.enabled = self.config.kick_enabled
        self.events.emit("state_kick", self.state.model_dump(mode="json"))

    def _get_session_token(self) -> str:
        creds = self.config.credentials.get("kick", {})
        token = str(creds.get("session_token") or "").strip()
        if token:
            return urllib.parse.unquote(token)
        for c in creds.get("cookies") or []:
            if isinstance(c, dict) and c.get("name") == "session_token" and c.get("value"):
                return urllib.parse.unquote(str(c["value"]))
        return ""

    def _build_headers(
        self,
        *,
        authenticated: bool = True,
        with_client_token: bool = False,
        extra: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {
            "User-Agent": KICK_UA,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://kick.com",
            "Referer": "https://kick.com/",
        }
        if with_client_token:
            headers["X-Client-Token"] = KICK_CLIENT_TOKEN
        if authenticated:
            token = self._get_session_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
            cookies_list = self.config.credentials.get("kick", {}).get("cookies") or []
            cookie_parts = []
            has_sess_cookie = False
            for c in cookies_list:
                if isinstance(c, dict) and c.get("name") and c.get("value") is not None:
                    cookie_parts.append(f"{c['name']}={c['value']}")
                    if c["name"] == "session_token":
                        has_sess_cookie = True
            if token and not has_sess_cookie:
                cookie_parts.append(f"session_token={token}")
            if cookie_parts:
                headers["Cookie"] = "; ".join(cookie_parts)
        if extra:
            headers.update(extra)
        return headers

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        authenticated: bool = True,
        with_client_token: bool = False,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        headers = self._build_headers(
            authenticated=authenticated,
            with_client_token=with_client_token,
            extra={"Content-Type": "application/json"} if json_body is not None else None,
        )
        if HAS_CURL_CFFI:
            if self._curl_session is None:
                self._curl_session = CurlAsyncSession(
                    impersonate="chrome124",
                    timeout=20,
                    proxies={"http": self.config.proxy, "https": self.config.proxy}
                    if self.config.proxy
                    else None,
                )
            resp = await self._curl_session.request(
                method.upper(),
                url,
                headers=headers,
                json=json_body,
            )
            if resp.status_code >= 400:
                text_preview = resp.text[:250] if resp.text else ""
                raise RuntimeError(f"Kick HTTP {resp.status_code} on {url}: {text_preview}")
            return resp.json()
        else:
            if self._aio_session is None or self._aio_session.closed:
                self._aio_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=20)
                )
            async with self._aio_session.request(
                method.upper(),
                url,
                headers=headers,
                json=json_body,
                proxy=self.config.proxy or None,
            ) as resp:
                if resp.status >= 400:
                    text_preview = (await resp.text())[:250]
                    raise RuntimeError(f"Kick HTTP {resp.status} on {url}: {text_preview}")
                return await resp.json()

    async def validate_auth(self) -> bool:
        token = self._get_session_token()
        if not token:
            self.state.authenticated = False
            self._emit_state()
            return False

        try:
            data = await self._request_json("GET", "https://kick.com/api/v1/user", authenticated=True)
            if isinstance(data, dict):
                user_obj = data.get("user") if isinstance(data.get("user"), dict) else data
                uid = user_obj.get("id")
                uname = user_obj.get("username") or user_obj.get("slug")
                if uid or uname:
                    self.config.credentials["kick"]["user_id"] = str(uid or "")
                    self.config.credentials["kick"]["username"] = str(uname or "KickUser")
                    self.config.credentials["kick"]["session_token"] = token
                    self.config.save_credentials()
                    self.state.authenticated = True
                    self.state.user_id = str(uid or "")
                    self.state.username = str(uname or "KickUser")
                    self.state.auth_flow = None
                    self._emit_state()
                    return True
        except Exception as e:
            # If /api/v1/user is temporarily blocked or fails, check if /drops/progress works with the token
            try:
                prog = await self._request_json(
                    "GET",
                    "https://web.kick.com/api/v1/drops/progress",
                    authenticated=True,
                    with_client_token=True,
                )
                if isinstance(prog, dict) and ("data" in prog or "progress" in prog):
                    self.state.authenticated = True
                    if not self.state.username:
                        self.state.username = self.config.credentials["kick"].get("username") or "KickUser"
                    self.state.auth_flow = None
                    self._emit_state()
                    return True
            except Exception:
                pass
            self.events.log(f"Kick authentication check failed: {e}", platform="kick", level="warn")

        self.state.authenticated = False
        self._emit_state()
        return False

    async def start_tv_device_login(self) -> dict[str, Any]:
        """
        Starts Kick's Smart-TV Link login flow (https://kick.com/tv/login?uuid=...&code=...)
        so a headless Linux host can obtain a Kick session_token without opening a local browser.
        """
        if self._auth_task and not self._auth_task.done():
            self._auth_task.cancel()

        device_uuid = str(uuid.uuid4()).upper()
        code = str(random.randint(100000, 999999))
        verification_uri = f"https://kick.com/tv/login?uuid={device_uuid}&code={code}"
        expires_in = 300

        flow_info = {
            "user_code": code,
            "uuid": device_uuid,
            "verification_uri": verification_uri,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
            "status": "waiting_for_approval",
        }
        self.state.auth_flow = flow_info
        self._emit_state()
        self.events.log(
            f"Kick TV Login: Open {verification_uri} (while signed into Kick) and confirm code {code}",
            platform="kick",
        )

        async def _poll() -> None:
            deadline = time.time() + expires_in
            auth_url = f"https://kick.com/api/tv/link/authenticate/{urllib.parse.quote(device_uuid)}"
            while time.time() < deadline:
                await asyncio.sleep(5)
                try:
                    resp = await self._request_json(
                        "POST",
                        auth_url,
                        authenticated=False,
                        json_body={"key": code},
                    )
                    if isinstance(resp, dict):
                        token = resp.get("token") or (resp.get("data") or {}).get("token")
                        if token and isinstance(token, str):
                            self.config.credentials["kick"]["session_token"] = urllib.parse.unquote(token)
                            self.config.save_credentials()
                            await self.validate_auth()
                            self.events.log(
                                f"Kick TV device login approved! Signed in as {self.state.username}.",
                                platform="kick",
                            )
                            self.wake()
                            return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # 403 "Invalid setup UUID and Key" is expected before user approves
                    pass

            self.state.auth_flow = None
            self._emit_state()
            self.events.log("Kick TV login code expired.", platform="kick", level="warn")

        self._auth_task = asyncio.create_task(_poll())
        return flow_info

    async def set_manual_credentials(
        self,
        session_token: Optional[str] = None,
        cookies_json: Optional[str] = None,
    ) -> bool:
        creds = self.config.credentials["kick"]
        if cookies_json:
            try:
                parsed = json.loads(cookies_json)
                if isinstance(parsed, list):
                    creds["cookies"] = parsed
                    for c in parsed:
                        if isinstance(c, dict) and c.get("name") == "session_token" and c.get("value"):
                            creds["session_token"] = urllib.parse.unquote(str(c["value"]))
                elif isinstance(parsed, dict):
                    if "session_token" in parsed:
                        creds["session_token"] = urllib.parse.unquote(str(parsed["session_token"]))
            except Exception as e:
                self.events.log(f"Invalid cookies JSON: {e}", platform="kick", level="error")
                return False

        if session_token:
            clean = session_token.strip().removeprefix("Bearer ").strip()
            creds["session_token"] = urllib.parse.unquote(clean)

        self.config.save_credentials()
        ok = await self.validate_auth()
        if ok:
            self.events.log(f"Kick credentials verified for {self.state.username}!", platform="kick")
            self.wake()
        return ok

    async def logout(self) -> None:
        if self._auth_task and not self._auth_task.done():
            self._auth_task.cancel()
        self.config.credentials["kick"] = {
            "session_token": "",
            "user_id": "",
            "username": "",
            "cookies": [],
        }
        self.config.save_credentials()
        self.state.authenticated = False
        self.state.username = ""
        self.state.user_id = ""
        self.state.watching_channel = None
        self.state.active_campaign = None
        self.state.active_drop = None
        self.state.status = MinerStatus.AUTH_REQUIRED
        self.state.status_message = "Authentication required"
        self.campaigns.clear()
        self._emit_state()
        self.events.log("Logged out of Kick.", platform="kick")

    def _collect_records(self, raw: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
        if isinstance(raw, list):
            return [r for r in raw if isinstance(r, dict)]
        if not isinstance(raw, dict):
            return []
        data = raw.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        out: list[dict[str, Any]] = []
        for k in keys:
            if isinstance(raw.get(k), list):
                out.extend(r for r in raw[k] if isinstance(r, dict))
            if isinstance(data, dict) and isinstance(data.get(k), list):
                out.extend(r for r in data[k] if isinstance(r, dict))
        return out

    async def fetch_campaigns_and_progress(self) -> None:
        self.state.status = MinerStatus.FETCHING
        self.state.status_message = "Fetching Kick drop campaigns & progress..."
        self._emit_state()

        try:
            camp_resp = await self._request_json(
                "GET",
                "https://web.kick.com/api/v1/drops/campaigns",
                authenticated=self.state.authenticated,
            )
            raw_campaigns = self._collect_records(
                camp_resp, ("campaigns", "active", "current", "upcoming")
            )

            progress_records: list[dict[str, Any]] = []
            if self.state.authenticated:
                try:
                    prog_resp = await self._request_json(
                        "GET",
                        "https://web.kick.com/api/v1/drops/progress",
                        authenticated=True,
                        with_client_token=True,
                    )
                    progress_records = self._collect_records(
                        prog_resp, ("progress", "campaigns", "active", "current", "completed")
                    )
                except Exception as pe:
                    self.events.log(
                        f"Could not fetch Kick progress (continuing with campaigns): {pe}",
                        platform="kick",
                        level="warn",
                    )

            progress_by_cid: dict[str, dict[str, Any]] = {}
            for p in progress_records:
                cid = str(p.get("id") or p.get("campaign_id") or p.get("drop_campaign_id") or "")
                if cid:
                    progress_by_cid[cid] = p

            parsed_campaigns: list[DropCampaign] = []
            new_by_id: dict[str, DropCampaign] = {}

            for c in raw_campaigns:
                cid = str(c.get("id") or "")
                if not cid:
                    continue
                prog = progress_by_cid.get(cid, {})
                cat = c.get("category") if isinstance(c.get("category"), dict) else {}
                if not cat and isinstance(prog.get("category"), dict):
                    cat = prog["category"]

                game_name = str(cat.get("name") or "Unknown Game")
                game_slug = str(cat.get("slug") or "")
                game_id = str(
                    c.get("category_id")
                    or c.get("game_id")
                    or cat.get("id")
                    or (prog.get("category") or {}).get("id")
                    or ""
                )
                game_img = str(cat.get("image_url") or "")

                starts_at = parse_kick_iso(c.get("starts_at") or c.get("startAt"))
                ends_at = parse_kick_iso(c.get("ends_at") or c.get("endAt"))
                raw_status = str(c.get("status") or "active").lower()

                link_url = str(prog.get("connect_url") or c.get("connect_url") or "")
                user_connected = prog.get("user_app_connected")
                account_linked = False if (user_connected is False and bool(link_url)) else True

                channels_list: list[ChannelInfo] = []
                for ch_raw in c.get("channels") or []:
                    if not isinstance(ch_raw, dict):
                        continue
                    user_node = ch_raw.get("user") if isinstance(ch_raw.get("user"), dict) else {}
                    slug = str(
                        ch_raw.get("slug")
                        or ch_raw.get("username")
                        or user_node.get("slug")
                        or user_node.get("username")
                        or ""
                    ).lower()
                    if not slug:
                        continue
                    uname = str(user_node.get("username") or slug)
                    channels_list.append(
                        ChannelInfo(
                            id=str(ch_raw.get("id") or slug),
                            username=slug,
                            display_name=uname,
                            platform=Platform.KICK,
                            url=f"https://kick.com/{slug}",
                            game_name=game_name,
                            game_id=game_id,
                            game_slug=game_slug,
                            acl_based=True,
                            profile_picture=str(
                                user_node.get("profile_picture") or ch_raw.get("profile_picture") or ""
                            ),
                        )
                    )

                prog_units = int(prog.get("progress_units") or 0)
                prog_rewards_map: dict[str, dict[str, Any]] = {}
                for pr in prog.get("rewards") or []:
                    if isinstance(pr, dict):
                        rid = str(pr.get("id") or pr.get("reward_id") or pr.get("drop_id") or "")
                        if rid:
                            prog_rewards_map[rid] = pr

                drops_list: list[DropReward] = []
                for r_raw in c.get("rewards") or c.get("drops") or []:
                    if not isinstance(r_raw, dict) or r_raw.get("id") is None:
                        continue
                    rid = str(r_raw["id"])
                    pr_match = prog_rewards_map.get(rid, {})

                    req_units = int(
                        r_raw.get("required_units")
                        or r_raw.get("required_minutes")
                        or pr_match.get("required_units")
                        or 0
                    )
                    # Cap required minutes at campaign window if shorter (flash drops)
                    if starts_at and ends_at and req_units > 0:
                        win_mins = int((ends_at - starts_at).total_seconds() // 60)
                        if 0 < win_mins < req_units:
                            req_units = win_mins

                    is_claimed = bool(
                        r_raw.get("claimed")
                        or r_raw.get("is_claimed")
                        or pr_match.get("claimed")
                        or pr_match.get("is_claimed")
                    )

                    if is_claimed:
                        cur_mins = req_units
                    elif prog_units > 0:
                        cur_mins = min(prog_units, req_units) if req_units > 0 else prog_units
                    elif pr_match.get("progress_units") is not None:
                        cur_mins = int(pr_match["progress_units"])
                    elif pr_match.get("progress") is not None and req_units > 0:
                        frac = float(pr_match["progress"])
                        if frac > 1.0:
                            frac = frac / 100.0
                        cur_mins = int(round(frac * req_units))
                    else:
                        cur_mins = 0

                    claim_id = pr_match.get("claim_id") or prog.get("claim_id")
                    can_claim = bool(
                        not is_claimed
                        and req_units > 0
                        and (
                            cur_mins >= req_units
                            or str(pr_match.get("status") or "").lower() == "claimable"
                        )
                    )

                    raw_img = str(r_raw.get("image_url") or r_raw.get("image") or "")
                    if raw_img and not raw_img.startswith("http"):
                        raw_img = f"{KICK_ASSET_BASE}/{raw_img.lstrip('/')}"

                    drops_list.append(
                        DropReward(
                            id=rid,
                            name=str(r_raw.get("name") or r_raw.get("title") or f"Reward {rid}"),
                            platform=Platform.KICK,
                            campaign_id=cid,
                            campaign_name=str(c.get("name") or c.get("title") or game_name),
                            game_name=game_name,
                            image_url=raw_img or game_img,
                            required_minutes=req_units,
                            current_minutes=cur_mins,
                            is_claimed=is_claimed,
                            can_claim=can_claim,
                            claim_id=str(claim_id) if claim_id else None,
                            starts_at=starts_at,
                            ends_at=ends_at,
                        )
                    )

                camp_obj = DropCampaign(
                    id=cid,
                    name=str(c.get("name") or c.get("title") or game_name),
                    platform=Platform.KICK,
                    game_id=game_id,
                    game_name=game_name,
                    game_slug=game_slug,
                    image_url=game_img,
                    status=raw_status,
                    starts_at=starts_at,
                    ends_at=ends_at,
                    account_linked=account_linked,
                    account_link_url=link_url,
                    is_global_drop=len(channels_list) == 0,
                    allowed_channels=channels_list,
                    drops=drops_list,
                    progress_units=prog_units,
                )
                parsed_campaigns.append(camp_obj)
                new_by_id[cid] = camp_obj

            parsed_campaigns.sort(
                key=lambda c: (
                    0 if c.is_active_time and not c.finished else (1 if not c.finished else 2),
                    c.ends_at or datetime.max.replace(tzinfo=timezone.utc),
                )
            )
            self.campaigns = parsed_campaigns
            self._campaigns_by_id = new_by_id
            self._last_inventory_fetch = time.time()
            self._last_progress_fetch = time.time()

            self.events.log(
                f"Fetched {len(self.campaigns)} Kick campaigns ({sum(1 for c in self.campaigns if c.is_active_time and not c.finished)} active).",
                platform="kick",
            )
            self.events.emit("campaigns_updated", {"platform": "kick"})

            if self.config.auto_claim and self.state.authenticated:
                await self.claim_all_ready()

        except Exception as e:
            self.events.log(f"Error fetching Kick campaigns: {e}", platform="kick", level="error")

    async def claim_drop(self, camp: DropCampaign, drop: DropReward) -> bool:
        if drop.is_claimed:
            return True
        supp_key = f"{camp.id}:{drop.id}"
        if supp_key in self._suppressed_claims:
            return False

        payload: dict[str, Any] = {
            "campaign_id": camp.id,
            "reward_id": drop.id,
        }
        if drop.claim_id:
            payload["claim_id"] = drop.claim_id

        try:
            resp = await self._request_json(
                "POST",
                "https://web.kick.com/api/v1/drops/claim",
                authenticated=True,
                with_client_token=True,
                json_body=payload,
            )
            success = False
            if isinstance(resp, dict):
                if resp.get("success") is True:
                    success = True
                elif isinstance(resp.get("message"), str) and "success" in resp["message"].lower():
                    success = True
                elif isinstance(resp.get("data"), dict) and resp["data"].get("id") is not None:
                    success = True
                elif resp.get("connect_url") or (isinstance(resp.get("data"), dict) and resp["data"].get("connect_url")):
                    link_url = resp.get("connect_url") or resp["data"].get("connect_url")
                    self._suppressed_claims.add(supp_key)
                    self.events.log(
                        f"Cannot claim Kick drop '{drop.name}' yet — link your game account at {link_url}",
                        platform="kick",
                        level="warn",
                    )
                    return False

            if success:
                drop.is_claimed = True
                drop.can_claim = False
                drop.current_minutes = drop.required_minutes
                self.state.drops_claimed_session += 1
                rec = self.config.record_claim(
                    Platform.KICK,
                    camp.name,
                    camp.game_name,
                    drop.name,
                    drop.image_url,
                )
                self.events.log(
                    f"🎁 Claimed Kick Drop: {drop.name} ({camp.game_name})!",
                    platform="kick",
                    category="claim",
                )
                self.events.emit("claim", rec.model_dump(mode="json"))
                self.events.emit("campaigns_updated", {"platform": "kick"})
                self._emit_state()
                return True
        except Exception as e:
            if not camp.account_linked and camp.account_link_url:
                self._suppressed_claims.add(supp_key)
                self.events.log(
                    f"Kick drop '{drop.name}' requires account linking: {camp.account_link_url}",
                    platform="kick",
                    level="warn",
                )
            else:
                self.events.log(f"Kick claim failed for {drop.name}: {e}", platform="kick", level="warn")
        return False

    async def claim_all_ready(self) -> int:
        count = 0
        for camp in self.campaigns:
            for drop in camp.drops:
                if not drop.is_claimed and drop.can_claim:
                    if await self.claim_drop(camp, drop):
                        count += 1
        return count

    async def check_channel_info(self, username: str) -> Optional[ChannelInfo]:
        slug = username.strip().strip("/").split("/")[-1].lower()
        if not slug:
            return None
        for endpoint in (
            f"https://kick.com/api/v2/channels/{urllib.parse.quote(slug)}/info",
            f"https://kick.com/api/v2/channels/{urllib.parse.quote(slug)}",
        ):
            try:
                data = await self._request_json("GET", endpoint, authenticated=False)
                if not isinstance(data, dict):
                    continue
                ch_id = str(data.get("id") or slug)
                livestream = data.get("livestream")
                if not isinstance(livestream, dict):
                    return ChannelInfo(
                        id=ch_id,
                        username=slug,
                        display_name=str((data.get("user") or {}).get("username") or slug),
                        platform=Platform.KICK,
                        url=f"https://kick.com/{slug}",
                        is_live=False,
                    )
                is_live = bool(livestream.get("is_live", True))
                cats = livestream.get("categories") or []
                cat = cats[0] if (isinstance(cats, list) and cats) else (livestream.get("category") or {})
                if not isinstance(cat, dict):
                    cat = {}
                return ChannelInfo(
                    id=ch_id,
                    username=slug,
                    display_name=str((data.get("user") or {}).get("username") or slug),
                    platform=Platform.KICK,
                    url=f"https://kick.com/{slug}",
                    is_live=is_live,
                    viewers=int(livestream.get("viewer_count") or 0),
                    game_name=str(cat.get("name") or ""),
                    game_id=str(cat.get("id") or ""),
                    game_slug=str(cat.get("slug") or ""),
                    title=str(livestream.get("session_title") or livestream.get("title") or ""),
                    broadcast_id=str(livestream.get("id") or "") if livestream.get("id") else None,
                    vod_id=str(livestream.get("vod_id") or "") if livestream.get("vod_id") else None,
                    drops_enabled=True,
                )
            except Exception:
                continue
        return None

    async def fetch_live_streamers_by_category(self, category_id: str, limit: int = 24) -> list[ChannelInfo]:
        if not category_id:
            return []
        url = (
            f"https://web.kick.com/api/v1/livestreams"
            f"?limit={limit}&sort=viewer_count_desc&category_id={urllib.parse.quote(str(category_id))}"
        )
        try:
            resp = await self._request_json("GET", url, authenticated=False)
            data_obj = resp.get("data", {}) if isinstance(resp, dict) else {}
            if isinstance(data_obj, dict):
                streams = data_obj.get("livestreams", [])
            elif isinstance(data_obj, list):
                streams = data_obj
            else:
                streams = []

            out: list[ChannelInfo] = []
            for st in streams[:limit]:
                if not isinstance(st, dict):
                    continue
                ch = st.get("channel") if isinstance(st.get("channel"), dict) else {}
                user = ch.get("user") if isinstance(ch.get("user"), dict) else {}
                slug = str(ch.get("slug") or user.get("username") or st.get("slug") or "").lower()
                if not slug:
                    continue
                cat = st.get("category") if isinstance(st.get("category"), dict) else {}
                out.append(
                    ChannelInfo(
                        id=str(ch.get("id") or slug),
                        username=slug,
                        display_name=str(user.get("username") or ch.get("username") or slug),
                        platform=Platform.KICK,
                        url=f"https://kick.com/{slug}",
                        is_live=True,
                        viewers=int(st.get("viewer_count") or 0),
                        game_name=str(cat.get("name") or ""),
                        game_id=str(cat.get("id") or category_id),
                        title=str(st.get("session_title") or st.get("title") or ""),
                        broadcast_id=str(st.get("id") or "") if st.get("id") else None,
                        acl_based=False,
                        drops_enabled=True,
                    )
                )
            return out
        except Exception as e:
            self.events.log(
                f"Failed to fetch Kick live streams for category {category_id}: {e}",
                platform="kick",
                level="warn",
            )
            return []

    def _can_earn_campaign(self, camp: DropCampaign) -> bool:
        if camp.finished or not camp.is_active_time:
            return False
        if camp.game_name in self.config.excluded_games:
            return False
        if (
            self.config.priority_mode == PriorityMode.PRIORITY_ONLY
            and camp.game_name not in self.config.kick_priority_games
        ):
            return False
        return any(not d.is_claimed and d.required_minutes > 0 for d in camp.drops)

    def get_prioritized_campaigns(self) -> list[DropCampaign]:
        eligible = [c for c in self.campaigns if self._can_earn_campaign(c)]
        priority_list = self.config.kick_priority_games
        mode = self.config.priority_mode

        def sort_key(c: DropCampaign) -> tuple[Any, ...]:
            p_idx = priority_list.index(c.game_name) if c.game_name in priority_list else 999999
            in_prog = 0 if c.progress_units > 0 else 1
            end_ts = c.ends_at.timestamp() if c.ends_at else float("inf")
            rem = c.remaining_minutes or 999999
            now_ts = datetime.now(timezone.utc).timestamp()
            avail = ((end_ts - now_ts) / 60.0) / rem if end_ts < float("inf") and rem > 0 else float("inf")

            if mode == PriorityMode.ENDING_SOONEST:
                return (p_idx if p_idx < 999999 else 999999, end_ts, in_prog, rem)
            elif mode == PriorityMode.LOW_AVAILABILITY:
                return (p_idx if p_idx < 999999 else 999999, avail, in_prog, end_ts)
            else:
                return (p_idx, in_prog, end_ts, rem)

        eligible.sort(key=sort_key)
        return eligible

    async def find_candidates_for_campaign(self, camp: DropCampaign) -> list[ChannelInfo]:
        candidates: list[ChannelInfo] = []
        if camp.allowed_channels:
            # Check up to 15 ACL channels concurrently in small batches
            for i in range(0, min(len(camp.allowed_channels), 18), 3):
                batch = camp.allowed_channels[i : i + 3]
                results = await asyncio.gather(
                    *(self.check_channel_info(ch.username) for ch in batch),
                    return_exceptions=True,
                )
                for ch_base, info in zip(batch, results):
                    if isinstance(info, ChannelInfo) and info.is_live:
                        if camp.game_id and info.game_id and info.game_id != camp.game_id:
                            continue
                        info.acl_based = True
                        if not info.game_name:
                            info.game_name = camp.game_name
                        candidates.append(info)
                if candidates:
                    break
            candidates.sort(key=lambda c: c.viewers, reverse=True)
            if candidates:
                return candidates

        if (camp.is_global_drop or not camp.allowed_channels) and camp.game_id:
            dir_streams = await self.fetch_live_streamers_by_category(camp.game_id, limit=15)
            for st in dir_streams:
                if not st.game_name:
                    st.game_name = camp.game_name
                candidates.append(st)

        return candidates

    async def select_best_channel(
        self,
    ) -> Optional[tuple[ChannelInfo, Optional[DropCampaign], Optional[ManualQueueItem]]]:
        # 1. Manual Queue (KickDropsMiner style queue with alternative channel switching)
        if self.config.prefer_manual_queue:
            for q_item in self.config.manual_queue:
                if q_item.platform == Platform.KICK and not q_item.finished:
                    info = await self.check_channel_info(q_item.username)
                    if info and info.is_live:
                        if (
                            not q_item.required_category_id
                            or not info.game_id
                            or info.game_id == q_item.required_category_id
                        ):
                            q_item.is_live = True
                            matched_camp = (
                                self._campaigns_by_id.get(q_item.campaign_id)
                                if q_item.campaign_id
                                else None
                            )
                            return info, matched_camp, q_item

                    # Try alternative channels in the queue item if main channel is offline/wrong category
                    q_item.is_live = False
                    for alt_url in q_item.alternative_channels:
                        alt_slug = alt_url.rstrip("/").split("/")[-1]
                        alt_info = await self.check_channel_info(alt_slug)
                        if alt_info and alt_info.is_live:
                            if (
                                not q_item.required_category_id
                                or not alt_info.game_id
                                or alt_info.game_id == q_item.required_category_id
                            ):
                                q_item.username = alt_info.username
                                q_item.url = alt_info.url
                                q_item.is_live = True
                                self.config.save()
                                self.events.log(
                                    f"Auto-switched Kick queue item to live alternative: {alt_info.display_name}",
                                    platform="kick",
                                )
                                matched_camp = (
                                    self._campaigns_by_id.get(q_item.campaign_id)
                                    if q_item.campaign_id
                                    else None
                                )
                                return alt_info, matched_camp, q_item

        # 2. Automatic Prioritized Campaigns
        for camp in self.get_prioritized_campaigns():
            cands = await self.find_candidates_for_campaign(camp)
            if cands:
                # Populate full live info (channel_id & broadcast_id) for the top candidate
                top = cands[0]
                if not top.broadcast_id or not top.id.isdigit():
                    detailed = await self.check_channel_info(top.username)
                    if detailed and detailed.is_live:
                        if not detailed.game_name:
                            detailed.game_name = camp.game_name
                        top = detailed
                return top, camp, None

        return None

    async def _fetch_viewer_token(self) -> str:
        resp = await self._request_json(
            "GET",
            "https://websockets.kick.com/viewer/v1/token",
            authenticated=True,
            with_client_token=True,
        )
        token = (resp.get("data") or {}).get("token") if isinstance(resp, dict) else None
        if not token and isinstance(resp, dict):
            token = resp.get("token")
        if not token:
            raise RuntimeError("Kick did not return a viewer WebSocket token")
        return str(token)

    def _update_active_drop_state(self, camp: Optional[DropCampaign] = None) -> None:
        ch = self.state.watching_channel
        if not ch:
            self.state.active_campaign = None
            self.state.active_drop = None
            self._emit_state()
            return

        target_camp = camp
        if target_camp is None:
            for c in self.get_prioritized_campaigns():
                if (
                    (c.game_id and ch.game_id and c.game_id == ch.game_id)
                    or (c.game_name and ch.game_name and c.game_name.lower() == ch.game_name.lower())
                    or any(ac.username.lower() == ch.username.lower() for ac in c.allowed_channels)
                ):
                    target_camp = c
                    break

        if target_camp:
            active_d = target_camp.active_drop
            self.state.active_campaign = {
                "id": target_camp.id,
                "name": target_camp.name,
                "game_name": target_camp.game_name,
                "image_url": target_camp.image_url,
                "overall_progress": round(target_camp.overall_progress * 100, 1),
            }
            if active_d:
                self.state.active_drop = {
                    "id": active_d.id,
                    "name": active_d.name,
                    "current_minutes": active_d.current_minutes,
                    "required_minutes": active_d.required_minutes,
                    "progress_percent": active_d.progress_percent,
                    "remaining_minutes": active_d.remaining_minutes,
                    "image_url": active_d.image_url,
                }
            else:
                self.state.active_drop = None
        self._emit_state()

    async def _watch_channel_session(
        self,
        channel: ChannelInfo,
        camp: Optional[DropCampaign],
        manual_item: Optional[ManualQueueItem],
    ) -> None:
        """
        Opens Kick's stream-less Viewer WebSocket (wss://websockets.kick.com/viewer/v1/connect)
        and sends channel_handshake (15s), ping (30s), and tracking.user.watch.livestream (120s).
        """
        # Ensure we have numeric channel_id and livestream_id
        if not channel.broadcast_id or not channel.id.isdigit():
            fresh = await self.check_channel_info(channel.username)
            if not fresh or not fresh.is_live or not fresh.broadcast_id:
                self.state.watching_channel = None
                return
            if not fresh.game_name and camp:
                fresh.game_name = camp.game_name
            channel = fresh
            self.state.watching_channel = channel

        viewer_token = await self._fetch_viewer_token()
        ws_url = f"wss://websockets.kick.com/viewer/v1/connect?token={urllib.parse.quote(viewer_token)}"
        ws_headers = {
            "Origin": "https://kick.com",
            "User-Agent": KICK_UA,
        }

        self.state.status = MinerStatus.WATCHING
        self.state.status_message = f"Mining on {channel.display_name} ({channel.game_name or 'Kick'})"
        self._update_active_drop_state(camp)

        async with websockets.connect(
            ws_url,
            additional_headers=ws_headers,
            open_timeout=15,
            ping_interval=None,
        ) as ws:
            last_handshake = 0.0
            last_ping = 0.0
            last_watch_event = 0.0
            last_live_verify = time.time()
            last_progress_poll = time.time()

            self.events.log(
                f"Kick stream-less Viewer WebSocket connected for {channel.display_name} (livestream {channel.broadcast_id})",
                platform="kick",
            )

            while (
                self._running
                and self.config.kick_enabled
                and self.state.authenticated
                and self.state.watching_channel
                and self.state.watching_channel.username.lower() == channel.username.lower()
            ):
                now = time.time()

                # 1. Handshake every 15s
                if now - last_handshake >= 15.0:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "channel_handshake",
                                "data": {"message": {"channelId": _numeric_or_str(channel.id)}},
                            }
                        )
                    )
                    last_handshake = now

                # 2. Ping every 30s
                if now - last_ping >= 30.0:
                    await ws.send(json.dumps({"type": "ping"}))
                    last_ping = now

                # 3. Watch livestream tracking event every 120s
                if now - last_watch_event >= self._watch_interval_sec:
                    msg_payload: dict[str, Any] = {
                        "name": "tracking.user.watch.livestream",
                        "channel_id": _numeric_or_str(channel.id),
                        "livestream_id": _numeric_or_str(channel.broadcast_id or "0"),
                    }
                    if channel.vod_id:
                        msg_payload["vod_id"] = channel.vod_id
                    await ws.send(
                        json.dumps(
                            {
                                "type": "user_event",
                                "data": {"message": msg_payload},
                            }
                        )
                    )
                    last_watch_event = now
                    self.state.last_watch_ping = datetime.now(timezone.utc).isoformat()
                    self._emit_state()

                # 4. Verify channel is still live and on the right category every 60s
                if now - last_live_verify >= 60.0:
                    last_live_verify = now
                    fresh_info = await self.check_channel_info(channel.username)
                    if not fresh_info or not fresh_info.is_live:
                        self.events.log(
                            f"Kick channel {channel.display_name} went offline, switching...",
                            platform="kick",
                        )
                        self.state.watching_channel = None
                        break
                    req_cat = (
                        manual_item.required_category_id
                        if manual_item and manual_item.required_category_id
                        else (camp.game_id if camp else None)
                    )
                    if req_cat and fresh_info.game_id and fresh_info.game_id != req_cat:
                        self.events.log(
                            f"Kick streamer {channel.display_name} changed category ({fresh_info.game_name}), switching...",
                            platform="kick",
                        )
                        self.state.watching_channel = None
                        break
                    channel.viewers = fresh_info.viewers
                    channel.broadcast_id = fresh_info.broadcast_id or channel.broadcast_id
                    channel.vod_id = fresh_info.vod_id or channel.vod_id

                    # Also check if a higher priority campaign is now available
                    if self.config.auto_switch and not manual_item:
                        best_now = await self.select_best_channel()
                        if best_now:
                            best_ch, best_camp, _ = best_now
                            if (
                                best_camp
                                and camp
                                and best_camp.id != camp.id
                                and best_ch.username.lower() != channel.username.lower()
                            ):
                                self.events.log(
                                    f"Higher priority Kick campaign '{best_camp.name}' available on {best_ch.display_name}, switching...",
                                    platform="kick",
                                )
                                self.state.watching_channel = best_ch
                                break

                # 5. Poll Kick drop progress every 60s to update UI and auto-claim
                if now - last_progress_poll >= 60.0:
                    last_progress_poll = now
                    await self.fetch_campaigns_and_progress()
                    if camp and camp.id in self._campaigns_by_id:
                        camp = self._campaigns_by_id[camp.id]
                        self._update_active_drop_state(camp)
                        if camp.finished:
                            self.events.log(
                                f"Completed all drops for Kick campaign '{camp.name}'! Switching...",
                                platform="kick",
                            )
                            self.state.watching_channel = None
                            break

                # 6. Wait 5s (and drain incoming websocket frames)
                try:
                    await asyncio.wait_for(ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

                if manual_item and not manual_item.finished:
                    manual_item.elapsed_seconds += 5
                    if (
                        manual_item.target_minutes > 0
                        and manual_item.elapsed_seconds >= manual_item.target_minutes * 60
                    ):
                        manual_item.finished = True
                        self.config.save()
                        self.events.log(
                            f"Completed manual Kick queue target ({manual_item.target_minutes}m) for {channel.display_name}!",
                            platform="kick",
                        )
                        self.state.watching_channel = None
                        break

    def wake(self) -> None:
        self._wake_event.set()

    async def switch_to_channel(self, username: str) -> bool:
        info = await self.check_channel_info(username)
        if not info or not info.is_live:
            return False
        self.state.watching_channel = info
        self.state.status = MinerStatus.WATCHING
        self.state.status_message = f"Watching {info.display_name} ({info.game_name})"
        self._update_active_drop_state()
        self.events.log(f"Switched Kick channel to {info.display_name} ({info.game_name})", platform="kick")
        self.wake()
        return True

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._main_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        self._wake_event.set()
        for t in (self._main_task, self._auth_task):
            if t and not t.done():
                t.cancel()
        if self._aio_session and not self._aio_session.closed:
            await self._aio_session.close()
        if self._curl_session is not None:
            try:
                await self._curl_session.close()
            except Exception:
                pass

    async def _run_loop(self) -> None:
        await self.validate_auth()
        while self._running:
            try:
                if not self.config.kick_enabled:
                    self.state.status = MinerStatus.PAUSED
                    self.state.status_message = "Kick miner is paused"
                    self.state.watching_channel = None
                    self._emit_state()
                    await self._wait_or_wake(10.0)
                    continue

                now = time.time()
                if (
                    not self.campaigns
                    or now - self._last_inventory_fetch >= self.config.inventory_refresh_minutes * 60
                ):
                    await self.fetch_campaigns_and_progress()

                if not self.state.authenticated:
                    self.state.status = MinerStatus.AUTH_REQUIRED
                    self.state.status_message = "Sign in to Kick (TV Code or Session Token) to start mining"
                    self.state.watching_channel = None
                    self._emit_state()
                    await self._wait_or_wake(10.0)
                    continue

                best = await self.select_best_channel()
                if best is None:
                    self.state.watching_channel = None
                    self.state.active_campaign = None
                    self.state.active_drop = None
                    self.state.status = MinerStatus.IDLE
                    self.state.status_message = "No active Kick drop streams available right now"
                    self._emit_state()
                    await self._wait_or_wake(30.0)
                    continue

                target_ch, target_camp, manual_item = best
                self.state.watching_channel = target_ch
                self.events.log(
                    f"Watching Kick stream: {target_ch.display_name} playing {target_ch.game_name or 'Kick'} ({target_ch.viewers} viewers)",
                    platform="kick",
                )

                await self._watch_channel_session(target_ch, target_camp, manual_item)
                await asyncio.sleep(2.0)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.events.log(f"Kick miner loop error: {e}", platform="kick", level="warn")
                await self._wait_or_wake(15.0)

    async def _wait_or_wake(self, timeout: float) -> None:
        self._wake_event.clear()
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
