from __future__ import annotations

import asyncio
import copy
import json
import math
import random
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from yarl import URL

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

TWITCH_DEVICE_CLIENT_ID = "ue6666qo983tsx6so1t0vnawi233wa"
TWITCH_DEVICE_UA = (
    "Mozilla/5.0 (Linux; Android 7.1; Smart Box C1) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
TWITCH_DEVICE_URL = "https://android.tv.twitch.tv"

TWITCH_ANDROID_CLIENT_ID = "kd1unb4b3q4t58fwlpcbzcbnm76a8fp"
TWITCH_ANDROID_UA = (
    "Dalvik/2.1.0 (Linux; U; Android 16; SM-S938B Build/BP2A.250605.031) "
    "tv.twitch.android.app/25.3.0/2503006"
)
TWITCH_CLIENT_URL = "https://www.twitch.tv"

CLIENT_PROFILES: dict[str, tuple[str, str]] = {
    TWITCH_DEVICE_CLIENT_ID: (TWITCH_DEVICE_URL, TWITCH_DEVICE_UA),
    TWITCH_ANDROID_CLIENT_ID: (TWITCH_CLIENT_URL, TWITCH_ANDROID_UA),
    "kimne78kx3ncx6brgo4mv6wki5h1ko": (
        TWITCH_CLIENT_URL,
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ),
}
DIMS_PATTERN = re.compile(r"-\d+x\d+(?=\.(?:jpg|png|gif)$)", re.I)

GQL_QUERIES: dict[str, dict[str, Any]] = {
    "GetStreamInfo": {
        "operationName": "VideoPlayerStreamInfoOverlayChannel",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "198492e0857f6aedead9665c81c5a06d67b25b58034649687124083ff288597d",
            }
        },
    },
    "ClaimCommunityPoints": {
        "operationName": "ClaimCommunityPoints",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "46aaeebe02c99afdf4fc97c7c0cba964124bf6b0af229395f1f6d1feed05b3d0",
            }
        },
    },
    "ClaimDrop": {
        "operationName": "DropsPage_ClaimDropRewards",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "a455deea71bdc9015b78eb49f4acfbce8baa7ccbedd28e549bb025bd0f751930",
            }
        },
    },
    "ChannelPointsContext": {
        "operationName": "ChannelPointsContext",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "374314de591e69925fce3ddc2bcf085796f56ebb8cad67a0daa3165c03adc345",
            }
        },
    },
    "Inventory": {
        "operationName": "Inventory",
        "variables": {"fetchRewardCampaigns": False},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "8337eb8541b314040b0edde0c09c5c7a2783ba1960aa9edfbf3bac16d0fec404",
            }
        },
    },
    "CurrentDrop": {
        "operationName": "DropCurrentSessionContext",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "4d06b702d25d652afb9ef835d2a550031f1cf762b193523a92166f40ea3d142b",
            }
        },
    },
    "Campaigns": {
        "operationName": "ViewerDropsDashboard",
        "variables": {"fetchRewardCampaigns": False},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "c16bb890cc8ce7647a96ee69cd313d423a378a3dedadf630a1017cde18975feb",
            }
        },
    },
    "CampaignDetails": {
        "operationName": "DropCampaignDetails",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "039277bf98f3130929262cc7c6efd9c141ca3749cb6dca442fc8ead9a53f77c1",
            }
        },
    },
    "AvailableDrops": {
        "operationName": "DropsHighlightService_AvailableDrops",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "782dad0f032942260171d2d80a654f88bdd0c5a9dddc392e9bc92218a0f42d20",
            }
        },
    },
    "PlaybackAccessToken": {
        "operationName": "PlaybackAccessToken",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "ed230aa1e33e07eebb8928504583da78a5173989fadfb1ac94be06a04f3cdbe9",
            }
        },
    },
    "GameDirectory": {
        "operationName": "DirectoryPage_Game",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "86bcceb4e8b1a51256ff8eed8bd8aae4acacf80d737efe904f84f3aeadf8cafd",
            }
        },
    },
    "SlugRedirect": {
        "operationName": "DirectoryGameRedirect",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "1f0300090caceec51f33c5e20647aceff9017f740f223c3c532ba6fa59f6b6cc",
            }
        },
    },
}


def make_gql(name: str, variables: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    op = copy.deepcopy(GQL_QUERIES[name])
    if variables is not None:
        if "variables" in op:
            op["variables"].update(variables)
        else:
            op["variables"] = variables
    return op


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


class TwitchMiner:
    """
    Headless Twitch Drops & Points Miner modeled after DevilXD/TwitchDropsMiner.
    Uses Twitch's Android App Client-Id, GQL API, PubSub WebSocket, and HLS HEAD requests.
    """

    def __init__(self, config: AppConfig, events: EventBus) -> None:
        self.config = config
        self.events = events
        self.state = PlatformMinerState(
            platform=Platform.TWITCH,
            enabled=config.twitch_enabled,
            watch_method="HLS Stream-less (HEAD)",
        )
        self.campaigns: list[DropCampaign] = []
        self.channels: dict[str, ChannelInfo] = {}
        self._drops_by_id: dict[str, DropReward] = {}
        self._campaigns_by_id: dict[str, DropCampaign] = {}
        self._game_slug_cache: dict[str, str] = {}
        self._stream_url_cache: dict[str, tuple[str, float]] = {}

        self._session: Optional[aiohttp.ClientSession] = None
        self._gql_semaphore = asyncio.Semaphore(5)
        self._running = False
        self._main_task: Optional[asyncio.Task[None]] = None
        self._watch_task: Optional[asyncio.Task[None]] = None
        self._pubsub_task: Optional[asyncio.Task[None]] = None
        self._auth_task: Optional[asyncio.Task[None]] = None
        self._wake_event = asyncio.Event()
        self._last_inventory_fetch: float = 0.0
        self._last_channel_check: float = 0.0

    def _emit_state(self) -> None:
        self.state.enabled = self.config.twitch_enabled
        self.events.emit("state_twitch", self.state.model_dump(mode="json"))

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(sock_connect=10, total=25)
            connector = aiohttp.TCPConnector(limit=40)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": TWITCH_DEVICE_UA},
            )
        return self._session

    def _ensure_ids(self) -> tuple[str, str]:
        creds = self.config.credentials["twitch"]
        if not creds.get("device_id"):
            creds["device_id"] = secrets.token_hex(16)
            self.config.save_credentials()
        if not creds.get("session_id"):
            creds["session_id"] = secrets.token_hex(8)
            self.config.save_credentials()
        return creds["device_id"], creds["session_id"]

    def _gql_headers(self) -> dict[str, str]:
        device_id, session_id = self._ensure_ids()
        creds = self.config.credentials["twitch"]
        token = creds.get("access_token", "")
        client_id = creds.get("client_id") or TWITCH_DEVICE_CLIENT_ID
        client_url, user_agent = CLIENT_PROFILES.get(
            client_id, (TWITCH_DEVICE_URL, TWITCH_DEVICE_UA)
        )
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US",
            "Client-Id": client_id,
            "Client-Session-Id": session_id,
            "X-Device-Id": device_id,
            "Origin": client_url,
            "Referer": client_url,
            "User-Agent": user_agent,
        }
        if token:
            headers["Authorization"] = f"OAuth {token}"
        return headers

    async def validate_auth(self) -> bool:
        creds = self.config.credentials["twitch"]
        token = creds.get("access_token", "").strip()
        if not token:
            self.state.authenticated = False
            self._emit_state()
            return False

        session = await self.get_session()
        try:
            async with session.get(
                "https://id.twitch.tv/oauth2/validate",
                headers={"Authorization": f"OAuth {token}"},
                proxy=self.config.proxy or None,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    creds["user_id"] = str(data.get("user_id", ""))
                    creds["username"] = str(data.get("login", ""))
                    if data.get("client_id"):
                        creds["client_id"] = str(data["client_id"])
                    self.config.save_credentials()
                    self.state.authenticated = True
                    self.state.user_id = creds["user_id"]
                    self.state.username = creds["username"]
                    self.state.auth_flow = None
                    self._emit_state()
                    return True
                elif resp.status == 401:
                    self.events.log("Twitch access token expired or invalid.", platform="twitch", level="warn")
                    creds["access_token"] = ""
                    self.config.save_credentials()
                    self.state.authenticated = False
                    self._emit_state()
                    return False
        except Exception as e:
            self.events.log(f"Error validating Twitch token: {e}", platform="twitch", level="warn")
        return self.state.authenticated

    async def start_device_login(self) -> dict[str, Any]:
        """Initiates Twitch OAuth2 Device Code flow and starts background polling."""
        if self._auth_task and not self._auth_task.done():
            self._auth_task.cancel()

        device_id, _ = self._ensure_ids()
        session = await self.get_session()
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US",
            "Cache-Control": "no-cache",
            "Client-Id": TWITCH_DEVICE_CLIENT_ID,
            "Origin": TWITCH_DEVICE_URL,
            "Referer": TWITCH_DEVICE_URL,
            "User-Agent": TWITCH_DEVICE_UA,
            "X-Device-Id": device_id,
        }
        payload = {"client_id": TWITCH_DEVICE_CLIENT_ID, "scopes": ""}
        async with session.post(
            "https://id.twitch.tv/oauth2/device",
            headers=headers,
            data=payload,
            proxy=self.config.proxy or None,
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Twitch device code request failed ({resp.status}): {text[:200]}")
            data = await resp.json()

        device_code = data["device_code"]
        user_code = data["user_code"]
        interval = int(data.get("interval", 5))
        expires_in = int(data.get("expires_in", 1800))
        verification_uri = data.get("verification_uri", "https://www.twitch.tv/activate")

        flow_info = {
            "user_code": user_code,
            "verification_uri": verification_uri,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
            "status": "waiting_for_approval",
        }
        self.state.auth_flow = flow_info
        self._emit_state()
        self.events.log(
            f"Twitch Login: Open {verification_uri} and enter code {user_code}",
            platform="twitch",
        )

        async def _poll() -> None:
            deadline = time.time() + expires_in
            token_payload = {
                "client_id": TWITCH_DEVICE_CLIENT_ID,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            }
            while time.time() < deadline:
                await asyncio.sleep(interval)
                try:
                    async with session.post(
                        "https://id.twitch.tv/oauth2/token",
                        headers=headers,
                        data=token_payload,
                        proxy=self.config.proxy or None,
                    ) as poll_resp:
                        if poll_resp.status == 200:
                            token_data = await poll_resp.json()
                            access_token = token_data.get("access_token")
                            if access_token:
                                self.config.credentials["twitch"]["access_token"] = access_token
                                self.config.credentials["twitch"]["client_id"] = TWITCH_DEVICE_CLIENT_ID
                                self.config.save_credentials()
                                await self.validate_auth()
                                self.events.log(
                                    f"Twitch logged in successfully as {self.state.username}!",
                                    platform="twitch",
                                )
                                self.wake()
                                return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
            self.state.auth_flow = None
            self._emit_state()
            self.events.log("Twitch device login code expired.", platform="twitch", level="warn")

        self._auth_task = asyncio.create_task(_poll())
        return flow_info

    async def set_manual_token(self, access_token: str) -> bool:
        clean_token = access_token.strip().removeprefix("OAuth ").removeprefix("Bearer ").strip()
        self.config.credentials["twitch"]["access_token"] = clean_token
        self.config.save_credentials()
        ok = await self.validate_auth()
        if ok:
            self.events.log(f"Twitch token verified for user {self.state.username}.", platform="twitch")
            self.wake()
        return ok

    async def logout(self) -> None:
        if self._auth_task and not self._auth_task.done():
            self._auth_task.cancel()
        self.config.credentials["twitch"] = {
            "access_token": "",
            "user_id": "",
            "username": "",
            "device_id": secrets.token_hex(16),
            "session_id": secrets.token_hex(8),
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
        self.events.log("Logged out of Twitch.", platform="twitch")

    async def gql_request(self, ops: dict[str, Any] | list[dict[str, Any]]) -> Any:
        session = await self.get_session()
        async with self._gql_semaphore:
            for attempt in range(3):
                try:
                    async with session.post(
                        "https://gql.twitch.tv/gql",
                        json=ops,
                        headers=self._gql_headers(),
                        proxy=self.config.proxy or None,
                    ) as resp:
                        if resp.status >= 500:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
                        data = await resp.json()
                        return data
                except Exception as e:
                    if attempt == 2:
                        raise e
                    await asyncio.sleep(1.5 * (attempt + 1))
                finally:
                    await asyncio.sleep(0.15)
        raise RuntimeError("Twitch GQL request failed after retries")

    def _merge_dict(self, primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
        merged = dict(secondary)
        for k, v in primary.items():
            if k in secondary and isinstance(v, dict) and isinstance(secondary[k], dict):
                merged[k] = self._merge_dict(v, secondary[k])
            else:
                merged[k] = v
        return merged

    async def fetch_inventory(self) -> None:
        if not self.state.authenticated:
            return
        self.state.status = MinerStatus.FETCHING
        self.state.status_message = "Fetching Twitch drops inventory & campaigns..."
        self._emit_state()

        try:
            inv_resp = await self.gql_request(make_gql("Inventory"))
            inv_node = (inv_resp.get("data") or {}).get("currentUser", {}).get("inventory") or {}
            ongoing_raw = inv_node.get("dropCampaignsInProgress") or []
            game_event_drops = inv_node.get("gameEventDrops") or []
            claimed_benefits: dict[str, datetime] = {}
            for b in game_event_drops:
                if b and b.get("id") and b.get("lastAwardedAt"):
                    dt = parse_iso(b["lastAwardedAt"])
                    if dt:
                        claimed_benefits[str(b["id"])] = dt

            inventory_map: dict[str, dict[str, Any]] = {
                c["id"]: c for c in ongoing_raw if isinstance(c, dict) and c.get("id")
            }

            camp_resp = await self.gql_request(make_gql("Campaigns"))
            available_list = (
                (camp_resp.get("data") or {}).get("currentUser", {}).get("dropCampaigns") or []
            )
            available_ids = [
                c["id"]
                for c in available_list
                if isinstance(c, dict)
                and c.get("id")
                and c.get("status") in ("ACTIVE", "UPCOMING")
            ]
            available_map = {
                c["id"]: c
                for c in available_list
                if isinstance(c, dict) and c.get("id") in available_ids
            }

            # Fetch extended details in chunks of 20
            user_id = self.state.user_id
            for i in range(0, len(available_ids), 20):
                chunk_ids = available_ids[i : i + 20]
                ops = [
                    make_gql(
                        "CampaignDetails",
                        {"channelLogin": str(user_id), "dropID": cid},
                    )
                    for cid in chunk_ids
                ]
                details_list = await self.gql_request(ops)
                if isinstance(details_list, list):
                    for item_resp in details_list:
                        c_data = (
                            ((item_resp or {}).get("data") or {})
                            .get("user", {})
                            .get("dropCampaign")
                        )
                        if c_data and c_data.get("id"):
                            cid = c_data["id"]
                            base = available_map.get(cid, {})
                            merged_c = self._merge_dict(base, c_data)
                            if cid in inventory_map:
                                inventory_map[cid] = self._merge_dict(inventory_map[cid], merged_c)
                            else:
                                inventory_map[cid] = merged_c

            parsed_campaigns: list[DropCampaign] = []
            new_drops_by_id: dict[str, DropReward] = {}
            new_campaigns_by_id: dict[str, DropCampaign] = {}

            for cid, cdata in inventory_map.items():
                game_obj = cdata.get("game")
                if not game_obj:
                    continue
                game_id = str(game_obj.get("id") or "")
                game_name = str(game_obj.get("displayName") or game_obj.get("name") or "Unknown Game")
                game_slug = str(game_obj.get("slug") or "")
                if game_name and game_slug:
                    self._game_slug_cache[game_name] = game_slug
                box_art = str(game_obj.get("boxArtURL") or "")
                image_url = DIMS_PATTERN.sub("", box_art)

                starts_at = parse_iso(cdata.get("startAt"))
                ends_at = parse_iso(cdata.get("endAt"))
                raw_status = str(cdata.get("status") or "ACTIVE").lower()
                linked = bool((cdata.get("self") or {}).get("isAccountConnected", False))
                link_url = str(cdata.get("accountLinkURL") or "")

                allow = cdata.get("allow") or {}
                acl_channels: list[ChannelInfo] = []
                if allow.get("channels") and allow.get("isEnabled", True):
                    for ch_raw in allow["channels"]:
                        if not isinstance(ch_raw, dict):
                            continue
                        login = str(ch_raw.get("name") or ch_raw.get("login") or "").lower()
                        if not login:
                            continue
                        acl_channels.append(
                            ChannelInfo(
                                id=str(ch_raw.get("id") or login),
                                username=login,
                                display_name=str(ch_raw.get("displayName") or login),
                                platform=Platform.TWITCH,
                                url=f"https://www.twitch.tv/{login}",
                                game_name=game_name,
                                game_id=game_id,
                                acl_based=True,
                            )
                        )

                drops_list: list[DropReward] = []
                for d_raw in cdata.get("timeBasedDrops") or []:
                    if not isinstance(d_raw, dict) or not d_raw.get("id"):
                        continue
                    did = str(d_raw["id"])
                    d_start = parse_iso(d_raw.get("startAt")) or starts_at
                    d_end = parse_iso(d_raw.get("endAt")) or ends_at
                    req_mins = int(d_raw.get("requiredMinutesWatched") or 0)

                    benefits = d_raw.get("benefitEdges") or []
                    b_names: list[str] = []
                    b_img = ""
                    b_ids: list[str] = []
                    for edge in benefits:
                        b_node = (edge or {}).get("benefit") or {}
                        if b_node.get("name"):
                            b_names.append(str(b_node["name"]))
                        if not b_img and b_node.get("imageAssetURL"):
                            b_img = str(b_node["imageAssetURL"])
                        if b_node.get("id"):
                            b_ids.append(str(b_node["id"]))

                    reward_name = ", ".join(b_names) if b_names else str(d_raw.get("name") or "Drop Reward")

                    claim_id: Optional[str] = None
                    is_claimed = False
                    cur_mins = 0
                    if "self" in d_raw and isinstance(d_raw["self"], dict):
                        self_edge = d_raw["self"]
                        claim_id = self_edge.get("dropInstanceID")
                        is_claimed = bool(self_edge.get("isClaimed", False))
                        cur_mins = int(self_edge.get("currentMinutesWatched") or 0)
                    elif b_ids and d_start and d_end:
                        awarded_dts = [claimed_benefits[bid] for bid in b_ids if bid in claimed_benefits]
                        if awarded_dts and all(d_start <= dt < d_end for dt in awarded_dts):
                            is_claimed = True

                    if is_claimed:
                        cur_mins = req_mins

                    can_claim = bool(
                        claim_id
                        and not is_claimed
                        and (not ends_at or datetime.now(timezone.utc) < ends_at + timedelta(hours=24))
                    )
                    preconds = [
                        str(p["id"])
                        for p in (d_raw.get("preconditionDrops") or [])
                        if isinstance(p, dict) and p.get("id")
                    ]

                    drop_obj = DropReward(
                        id=did,
                        name=reward_name,
                        platform=Platform.TWITCH,
                        campaign_id=str(cid),
                        campaign_name=str(cdata.get("name") or game_name),
                        game_name=game_name,
                        image_url=b_img or image_url,
                        required_minutes=req_mins,
                        current_minutes=cur_mins,
                        is_claimed=is_claimed,
                        can_claim=can_claim,
                        claim_id=claim_id,
                        precondition_drops=preconds,
                        starts_at=d_start,
                        ends_at=d_end,
                    )
                    drops_list.append(drop_obj)
                    new_drops_by_id[did] = drop_obj

                camp_obj = DropCampaign(
                    id=str(cid),
                    name=str(cdata.get("name") or game_name),
                    platform=Platform.TWITCH,
                    game_id=game_id,
                    game_name=game_name,
                    game_slug=game_slug,
                    image_url=image_url,
                    status=raw_status,
                    starts_at=starts_at,
                    ends_at=ends_at,
                    account_linked=linked,
                    account_link_url=link_url,
                    is_global_drop=len(acl_channels) == 0,
                    allowed_channels=acl_channels,
                    drops=drops_list,
                )
                parsed_campaigns.append(camp_obj)
                new_campaigns_by_id[camp_obj.id] = camp_obj

            # Sort active first, then by end time
            parsed_campaigns.sort(
                key=lambda c: (
                    0 if c.is_active_time and not c.finished else (1 if not c.finished else 2),
                    c.ends_at or datetime.max.replace(tzinfo=timezone.utc),
                )
            )
            self.campaigns = parsed_campaigns
            self._drops_by_id = new_drops_by_id
            self._campaigns_by_id = new_campaigns_by_id
            self._last_inventory_fetch = time.time()

            self.events.log(
                f"Fetched {len(self.campaigns)} Twitch campaigns ({sum(1 for c in self.campaigns if c.is_active_time and not c.finished)} active).",
                platform="twitch",
            )
            self.events.emit("campaigns_updated", {"platform": "twitch"})

            if self.config.auto_claim:
                await self.claim_all_ready()

        except Exception as e:
            self.events.log(f"Failed to fetch Twitch inventory: {e}", platform="twitch", level="error")

    async def claim_drop(self, drop: DropReward) -> bool:
        if drop.is_claimed:
            return True
        if not drop.claim_id:
            if self.state.user_id and drop.current_minutes >= drop.required_minutes > 0:
                drop.claim_id = f"{self.state.user_id}#{drop.campaign_id}#{drop.id}"
            else:
                return False

        try:
            resp = await self.gql_request(
                make_gql("ClaimDrop", {"input": {"dropInstanceID": drop.claim_id}})
            )
            data = (resp or {}).get("data") or {}
            claim_node = data.get("claimDropRewards")
            if claim_node and claim_node.get("status") in (
                "ELIGIBLE_FOR_ALL",
                "DROP_INSTANCE_ALREADY_CLAIMED",
            ):
                drop.is_claimed = True
                drop.can_claim = False
                drop.current_minutes = drop.required_minutes
                self.state.drops_claimed_session += 1
                rec = self.config.record_claim(
                    Platform.TWITCH,
                    drop.campaign_name,
                    drop.game_name,
                    drop.name,
                    drop.image_url,
                )
                self.events.log(
                    f"🎁 Claimed Twitch Drop: {drop.name} ({drop.game_name})!",
                    platform="twitch",
                    category="claim",
                )
                self.events.emit("claim", rec.model_dump(mode="json"))
                self.events.emit("campaigns_updated", {"platform": "twitch"})
                self._emit_state()
                return True
        except Exception as e:
            self.events.log(f"Error claiming Twitch drop {drop.name}: {e}", platform="twitch", level="warn")
        return False

    async def claim_all_ready(self) -> int:
        claimed_count = 0
        for camp in self.campaigns:
            for drop in camp.drops:
                if not drop.is_claimed and (
                    drop.can_claim
                    or (drop.claim_id and drop.current_minutes >= drop.required_minutes > 0)
                ):
                    if await self.claim_drop(drop):
                        claimed_count += 1
        return claimed_count

    async def _resolve_game_slug(self, game_name: str) -> str:
        if game_name in self._game_slug_cache:
            return self._game_slug_cache[game_name]
        try:
            resp = await self.gql_request(make_gql("SlugRedirect", {"name": game_name}))
            slug = (((resp or {}).get("data") or {}).get("game") or {}).get("slug")
            if slug:
                self._game_slug_cache[game_name] = str(slug)
                return str(slug)
        except Exception:
            pass
        slug_fallback = re.sub(r"[^a-z0-9]+", "-", game_name.lower()).strip("-")
        return slug_fallback

    def _can_earn_campaign(self, camp: DropCampaign) -> bool:
        if camp.finished or not camp.is_active_time:
            return False
        if camp.game_name in self.config.excluded_games:
            return False
        if (
            self.config.priority_mode == PriorityMode.PRIORITY_ONLY
            and camp.game_name not in self.config.twitch_priority_games
        ):
            return False
        return any(not d.is_claimed and d.required_minutes > 0 for d in camp.drops)

    def get_prioritized_campaigns(self) -> list[DropCampaign]:
        eligible = [c for c in self.campaigns if self._can_earn_campaign(c)]
        priority_list = self.config.twitch_priority_games
        mode = self.config.priority_mode

        def sort_key(c: DropCampaign) -> tuple[Any, ...]:
            p_idx = priority_list.index(c.game_name) if c.game_name in priority_list else 999999
            end_ts = c.ends_at.timestamp() if c.ends_at else float("inf")
            rem = c.remaining_minutes or 999999
            now_ts = datetime.now(timezone.utc).timestamp()
            avail = ((end_ts - now_ts) / 60.0) / rem if end_ts < float("inf") and rem > 0 else float("inf")

            if mode == PriorityMode.ENDING_SOONEST:
                return (p_idx if p_idx < 999999 else 999999, end_ts, rem)
            elif mode == PriorityMode.LOW_AVAILABILITY:
                return (p_idx if p_idx < 999999 else 999999, avail, end_ts)
            else:
                return (p_idx, end_ts, rem)

        eligible.sort(key=sort_key)
        return eligible

    async def check_channel_live(self, username: str) -> Optional[ChannelInfo]:
        login = username.lower().strip()
        try:
            resp = await self.gql_request(make_gql("GetStreamInfo", {"channel": login}))
            user = ((resp or {}).get("data") or {}).get("user")
            if not user or not user.get("stream"):
                return None
            stream = user["stream"]
            bcast = user.get("broadcastSettings") or {}
            game = bcast.get("game") or {}
            return ChannelInfo(
                id=str(user.get("id") or login),
                username=login,
                display_name=str(user.get("displayName") or login),
                platform=Platform.TWITCH,
                url=f"https://www.twitch.tv/{login}",
                is_live=True,
                viewers=int(stream.get("viewersCount") or 0),
                game_name=str(game.get("displayName") or game.get("name") or ""),
                game_id=str(game.get("id") or ""),
                title=str(bcast.get("title") or ""),
                broadcast_id=str(stream.get("id") or ""),
                drops_enabled=True,
            )
        except Exception:
            return None

    async def find_candidates_for_campaign(self, camp: DropCampaign) -> list[ChannelInfo]:
        candidates: list[ChannelInfo] = []
        # 1. Check ACL channels in batch if present
        if camp.allowed_channels:
            acl_slice = camp.allowed_channels[:40]
            for i in range(0, len(acl_slice), 20):
                batch = acl_slice[i : i + 20]
                ops = [make_gql("GetStreamInfo", {"channel": ch.username}) for ch in batch]
                try:
                    res_list = await self.gql_request(ops)
                    if isinstance(res_list, list):
                        for ch_base, item_resp in zip(batch, res_list):
                            user = ((item_resp or {}).get("data") or {}).get("user")
                            if not user or not user.get("stream"):
                                continue
                            stream = user["stream"]
                            bcast = user.get("broadcastSettings") or {}
                            game = bcast.get("game") or {}
                            g_name = str(game.get("displayName") or game.get("name") or "")
                            if g_name and camp.game_name and g_name.lower() != camp.game_name.lower():
                                if camp.game_name.lower() != "special events":
                                    continue
                            candidates.append(
                                ChannelInfo(
                                    id=str(user.get("id") or ch_base.id),
                                    username=ch_base.username,
                                    display_name=str(user.get("displayName") or ch_base.display_name),
                                    platform=Platform.TWITCH,
                                    url=f"https://www.twitch.tv/{ch_base.username}",
                                    is_live=True,
                                    viewers=int(stream.get("viewersCount") or 0),
                                    game_name=g_name or camp.game_name,
                                    game_id=str(game.get("id") or camp.game_id),
                                    title=str(bcast.get("title") or ""),
                                    broadcast_id=str(stream.get("id") or ""),
                                    acl_based=True,
                                    drops_enabled=True,
                                )
                            )
                except Exception:
                    pass
            candidates.sort(key=lambda c: c.viewers, reverse=True)
            if candidates:
                return candidates

        # 2. If global drop (or no ACL channels live & global allowed), query GameDirectory
        if camp.is_global_drop or not camp.allowed_channels:
            slug = camp.game_slug or await self._resolve_game_slug(camp.game_name)
            if slug:
                try:
                    resp = await self.gql_request(
                        make_gql(
                            "GameDirectory",
                            {
                                "limit": 25,
                                "slug": slug,
                                "imageWidth": 50,
                                "includeCostreaming": False,
                                "options": {
                                    "broadcasterLanguages": [],
                                    "freeformTags": None,
                                    "includeRestricted": ["SUB_ONLY_LIVE"],
                                    "recommendationsContext": {"platform": "web"},
                                    "sort": "VIEWER_COUNT",
                                    "systemFilters": ["DROPS_ENABLED"],
                                    "tags": [],
                                    "requestID": "JIRA-VXP-2397",
                                },
                                "sortTypeIsRecency": False,
                            },
                        )
                    )
                    edges = (
                        (((resp or {}).get("data") or {}).get("game") or {})
                        .get("streams", {})
                        .get("edges")
                        or []
                    )
                    for edge in edges:
                        node = (edge or {}).get("node") or {}
                        bcast = node.get("broadcaster") or {}
                        login = str(bcast.get("login") or "").lower()
                        if not login:
                            continue
                        candidates.append(
                            ChannelInfo(
                                id=str(bcast.get("id") or login),
                                username=login,
                                display_name=str(bcast.get("displayName") or login),
                                platform=Platform.TWITCH,
                                url=f"https://www.twitch.tv/{login}",
                                is_live=True,
                                viewers=int(node.get("viewersCount") or 0),
                                game_name=camp.game_name,
                                game_id=camp.game_id,
                                game_slug=slug,
                                title=str(node.get("title") or ""),
                                broadcast_id=str(node.get("id") or ""),
                                acl_based=False,
                                drops_enabled=True,
                            )
                        )
                except Exception as e:
                    self.events.log(
                        f"GameDirectory lookup failed for {camp.game_name}: {e}",
                        platform="twitch",
                        level="warn",
                    )

        candidates.sort(key=lambda c: (c.acl_based, c.viewers), reverse=True)
        return candidates

    async def send_watch_ping(self, channel: ChannelInfo) -> bool:
        """
        Stream-less watch simulation via HLS playlist + HEAD request on chunk,
        matching DevilXD/TwitchDropsMiner's Channel.send_watch().
        """
        session = await self.get_session()
        now = time.time()
        cached = self._stream_url_cache.get(channel.username)
        stream_url: Optional[str] = None
        if cached and now - cached[1] < 300:
            stream_url = cached[0]

        try:
            if not stream_url:
                token_resp = await self.gql_request(
                    make_gql(
                        "PlaybackAccessToken",
                        {
                            "isLive": True,
                            "isVod": False,
                            "login": channel.username,
                            "platform": "web",
                            "playerType": "site",
                            "vodID": "",
                        },
                    )
                )
                token_node = ((token_resp or {}).get("data") or {}).get("streamPlaybackAccessToken")
                if not token_node:
                    return False
                sig = token_node.get("signature")
                val = token_node.get("value")
                usher_url = (
                    f"https://usher.ttvnw.net/api/channel/hls/{channel.username}.m3u8"
                    f"?sig={sig}&token={URL.build(query={'t': val}).query_string[2:]}"
                )
                async with session.get(
                    URL(f"https://usher.ttvnw.net/api/channel/hls/{channel.username}.m3u8").with_query(
                        {"sig": sig, "token": val}
                    ),
                    proxy=self.config.proxy or None,
                ) as m3u8_resp:
                    if m3u8_resp.status >= 400:
                        return False
                    m3u8_text = await m3u8_resp.text()
                lines = [ln.strip() for ln in m3u8_text.strip().splitlines() if ln.strip().startswith("http")]
                if not lines:
                    return False
                stream_url = lines[-1]
                self._stream_url_cache[channel.username] = (stream_url, now)

            async with session.get(
                stream_url,
                headers={"Connection": "close"},
                proxy=self.config.proxy or None,
            ) as chunks_resp:
                if chunks_resp.status >= 400:
                    self._stream_url_cache.pop(channel.username, None)
                    return False
                chunks_text = await chunks_resp.text()

            chunk_urls = [
                ln.strip()
                for ln in chunks_text.strip().splitlines()
                if ln.strip().startswith("http")
            ]
            if not chunk_urls:
                self._stream_url_cache.pop(channel.username, None)
                return False

            async with session.head(
                chunk_urls[-1],
                proxy=self.config.proxy or None,
            ) as head_resp:
                ok = head_resp.status == 200
                if ok:
                    self.state.last_watch_ping = datetime.now(timezone.utc).isoformat()
                return ok
        except Exception:
            self._stream_url_cache.pop(channel.username, None)
            return False

    async def poll_current_drop_and_points(self, channel: ChannelInfo) -> None:
        """Queries CurrentDrop and ChannelPointsContext via GQL to sync progress & auto-claim points."""
        try:
            ops: list[dict[str, Any]] = []
            if channel.id.isdigit():
                ops.append(make_gql("CurrentDrop", {"channelID": str(channel.id), "channelLogin": ""}))
            if self.config.auto_claim_points:
                ops.append(make_gql("ChannelPointsContext", {"channelLogin": channel.username}))
            if not ops:
                return

            results = await self.gql_request(ops)
            if not isinstance(results, list):
                results = [results]

            for res in results:
                data = (res or {}).get("data") or {}
                if "currentUser" in data and data["currentUser"]:
                    session_ctx = data["currentUser"].get("dropCurrentSession")
                    if session_ctx and session_ctx.get("dropID"):
                        did = str(session_ctx["dropID"])
                        cur_mins = int(session_ctx.get("currentMinutesWatched") or 0)
                        drop = self._drops_by_id.get(did)
                        if drop and not drop.is_claimed:
                            if cur_mins != drop.current_minutes:
                                drop.current_minutes = cur_mins
                                if drop.current_minutes >= drop.required_minutes > 0:
                                    drop.can_claim = True
                                    if self.config.auto_claim:
                                        await self.claim_drop(drop)
                                self._update_active_drop_state()
                                self.events.emit("campaigns_updated", {"platform": "twitch"})

                if "community" in data and data["community"]:
                    ch_node = data["community"].get("channel") or {}
                    self_pts = (ch_node.get("self") or {}).get("communityPoints") or {}
                    avail_claim = self_pts.get("availableClaim")
                    if avail_claim and avail_claim.get("id") and ch_node.get("id"):
                        claim_id = str(avail_claim["id"])
                        ch_id = str(ch_node["id"])
                        await self.gql_request(
                            make_gql(
                                "ClaimCommunityPoints",
                                {"input": {"claimID": claim_id, "channelID": ch_id}},
                            )
                        )
                        self.state.points_claimed_session += 1
                        self.events.log(
                            f"✨ Claimed bonus channel points on {channel.display_name}!",
                            platform="twitch",
                        )
                        self._emit_state()
        except Exception:
            pass

    def _update_active_drop_state(self) -> None:
        ch = self.state.watching_channel
        if not ch:
            self.state.active_campaign = None
            self.state.active_drop = None
            self._emit_state()
            return

        for camp in self.get_prioritized_campaigns():
            if camp.game_name.lower() == ch.game_name.lower() or camp.game_name.lower() == "special events":
                active_d = camp.active_drop
                self.state.active_campaign = {
                    "id": camp.id,
                    "name": camp.name,
                    "game_name": camp.game_name,
                    "image_url": camp.image_url,
                    "overall_progress": round(camp.overall_progress * 100, 1),
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
                return
        self._emit_state()

    async def select_best_channel(self) -> Optional[ tuple[ChannelInfo, Optional[DropCampaign], Optional[ManualQueueItem]] ]:
        # 1. Check Manual Queue for Twitch items if enabled
        if self.config.prefer_manual_queue:
            for q_item in self.config.manual_queue:
                if q_item.platform == Platform.TWITCH and not q_item.finished:
                    ch_info = await self.check_channel_live(q_item.username)
                    if ch_info and ch_info.is_live:
                        q_item.is_live = True
                        matched_camp = (
                            self._campaigns_by_id.get(q_item.campaign_id)
                            if q_item.campaign_id
                            else None
                        )
                        return ch_info, matched_camp, q_item
                    else:
                        q_item.is_live = False

        # 2. Check Prioritized Campaigns
        for camp in self.get_prioritized_campaigns():
            candidates = await self.find_candidates_for_campaign(camp)
            if candidates:
                return candidates[0], camp, None

        return None

    async def _pubsub_loop(self) -> None:
        """Maintains a Twitch PubSub WebSocket connection for instant drop progress and claim events."""
        while self._running:
            if not self.config.twitch_enabled or not self.state.authenticated or not self.state.user_id:
                await asyncio.sleep(5)
                continue

            session = await self.get_session()
            try:
                async with session.ws_connect(
                    "wss://pubsub-edge.twitch.tv/v1",
                    heartbeat=30.0,
                    proxy=self.config.proxy or None,
                ) as ws:
                    uid = self.state.user_id
                    token = self.config.credentials["twitch"].get("access_token", "")
                    topics = [
                        f"user-drop-events.{uid}",
                        f"community-points-user-v1.{uid}",
                    ]
                    if self.state.watching_channel and self.state.watching_channel.id.isdigit():
                        topics.append(f"video-playback-by-id.{self.state.watching_channel.id}")

                    await ws.send_json(
                        {
                            "type": "LISTEN",
                            "nonce": secrets.token_hex(8),
                            "data": {"topics": topics, "auth_token": token},
                        }
                    )

                    last_ping = time.time()
                    while self._running and self.state.authenticated:
                        if time.time() - last_ping >= 180:
                            await ws.send_json({"type": "PING"})
                            last_ping = time.time()

                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
                        except asyncio.TimeoutError:
                            continue

                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        payload = json.loads(msg.data)
                        if payload.get("type") == "RECONNECT":
                            break
                        if payload.get("type") != "MESSAGE":
                            continue

                        topic = str((payload.get("data") or {}).get("topic") or "")
                        inner_raw = (payload.get("data") or {}).get("message")
                        if not inner_raw:
                            continue
                        inner = json.loads(inner_raw)
                        mtype = inner.get("type")

                        if topic.startswith("user-drop-events."):
                            d_data = inner.get("data") or {}
                            drop_id = str(d_data.get("drop_id") or "")
                            drop = self._drops_by_id.get(drop_id)
                            if mtype == "drop-progress" and drop:
                                cur = int(d_data.get("current_progress_min") or 0)
                                req = int(d_data.get("required_progress_min") or drop.required_minutes)
                                drop.current_minutes = cur
                                drop.required_minutes = req
                                self._update_active_drop_state()
                                self.events.emit("campaigns_updated", {"platform": "twitch"})
                            elif mtype == "drop-claim" and drop:
                                inst_id = str(d_data.get("drop_instance_id") or "")
                                if inst_id:
                                    drop.claim_id = inst_id
                                    drop.can_claim = True
                                if self.config.auto_claim:
                                    await self.claim_drop(drop)
                                    self.wake()
                        elif topic.startswith("video-playback-by-id."):
                            if mtype == "stream-down":
                                self.events.log(
                                    f"Stream went offline via PubSub: {self.state.watching_channel.display_name if self.state.watching_channel else ''}",
                                    platform="twitch",
                                )
                                self.wake()
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(10)

    def wake(self) -> None:
        self._wake_event.set()

    async def switch_to_channel(self, username: str) -> bool:
        ch = await self.check_channel_live(username)
        if not ch or not ch.is_live:
            return False
        self.state.watching_channel = ch
        self.state.status = MinerStatus.WATCHING
        self.state.status_message = f"Watching {ch.display_name} ({ch.game_name})"
        self._update_active_drop_state()
        self.events.log(f"Switched Twitch channel to {ch.display_name} ({ch.game_name})", platform="twitch")
        return True

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._main_task = asyncio.create_task(self._run_loop())
        self._pubsub_task = asyncio.create_task(self._pubsub_loop())

    async def stop(self) -> None:
        self._running = False
        self._wake_event.set()
        for t in (self._main_task, self._pubsub_task, self._auth_task):
            if t and not t.done():
                t.cancel()
        if self._session and not self._session.closed:
            await self._session.close()

    async def _run_loop(self) -> None:
        await self.validate_auth()
        while self._running:
            try:
                if not self.config.twitch_enabled:
                    self.state.status = MinerStatus.PAUSED
                    self.state.status_message = "Twitch miner is paused"
                    self.state.watching_channel = None
                    self._emit_state()
                    await self._wait_or_wake(10.0)
                    continue

                if not self.state.authenticated:
                    self.state.status = MinerStatus.AUTH_REQUIRED
                    self.state.status_message = "Sign in to Twitch to start mining"
                    self.state.watching_channel = None
                    self._emit_state()
                    await self._wait_or_wake(10.0)
                    continue

                now = time.time()
                # Refresh inventory periodically
                if (
                    not self.campaigns
                    or now - self._last_inventory_fetch >= self.config.inventory_refresh_minutes * 60
                ):
                    await self.fetch_inventory()

                # Evaluate channel selection / auto-switch
                if (
                    self.state.watching_channel is None
                    or now - self._last_channel_check >= self.config.channel_check_seconds
                ):
                    self._last_channel_check = now
                    best = await self.select_best_channel()
                    if best is None:
                        self.state.watching_channel = None
                        self.state.active_campaign = None
                        self.state.active_drop = None
                        self.state.status = MinerStatus.IDLE
                        self.state.status_message = "No active Twitch drop streams available right now"
                        self._emit_state()
                        await self._wait_or_wake(30.0)
                        continue

                    target_ch, target_camp, manual_item = best
                    curr_ch = self.state.watching_channel
                    if curr_ch is None or (
                        self.config.auto_switch and curr_ch.username.lower() != target_ch.username.lower()
                    ):
                        # If current channel is still live and playing the same highest-priority game, stay on it
                        should_change = True
                        if (
                            curr_ch is not None
                            and target_camp is not None
                            and curr_ch.game_name.lower() == target_camp.game_name.lower()
                        ):
                            live_curr = await self.check_channel_live(curr_ch.username)
                            if (
                                live_curr
                                and live_curr.is_live
                                and live_curr.game_name.lower() == target_camp.game_name.lower()
                                and (curr_ch.acl_based or not target_ch.acl_based)
                            ):
                                self.state.watching_channel = live_curr
                                should_change = False

                        if should_change:
                            self.state.watching_channel = target_ch
                            self.events.log(
                                f"Watching Twitch stream: {target_ch.display_name} playing {target_ch.game_name} ({target_ch.viewers} viewers)",
                                platform="twitch",
                            )

                # Send watch heartbeat (every 20 seconds)
                ch = self.state.watching_channel
                if ch:
                    self.state.status = MinerStatus.WATCHING
                    self.state.status_message = f"Mining on {ch.display_name} ({ch.game_name})"
                    self._update_active_drop_state()

                    ok = await self.send_watch_ping(ch)
                    if not ok:
                        # Stream may have gone offline
                        live_check = await self.check_channel_live(ch.username)
                        if not live_check or not live_check.is_live:
                            self.events.log(
                                f"Twitch channel {ch.display_name} went offline, switching...",
                                platform="twitch",
                            )
                            self.state.watching_channel = None
                            self._last_channel_check = 0.0
                            continue
                    else:
                        # Update manual queue item elapsed time if applicable
                        for q_item in self.config.manual_queue:
                            if (
                                q_item.platform == Platform.TWITCH
                                and not q_item.finished
                                and q_item.username.lower() == ch.username.lower()
                            ):
                                q_item.elapsed_seconds += 20
                                if (
                                    q_item.target_minutes > 0
                                    and q_item.elapsed_seconds >= q_item.target_minutes * 60
                                ):
                                    q_item.finished = True
                                    self.config.save()
                                    self.events.log(
                                        f"Manual queue target ({q_item.target_minutes}m) reached for {ch.display_name}!",
                                        platform="twitch",
                                    )
                                    self.state.watching_channel = None
                                    self._last_channel_check = 0.0

                    await self.poll_current_drop_and_points(ch)

                await self._wait_or_wake(20.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.events.log(f"Twitch miner loop error: {e}", platform="twitch", level="error")
                await self._wait_or_wake(15.0)

    async def _wait_or_wake(self, timeout: float) -> None:
        self._wake_event.clear()
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
