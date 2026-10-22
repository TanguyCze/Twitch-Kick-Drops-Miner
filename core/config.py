from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Optional

from .models import ClaimRecord, ManualQueueItem, Platform, PriorityMode

DATA_DIR = Path(os.environ.get("MINER_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
CONFIG_PATH = DATA_DIR / "config.json"
CREDENTIALS_PATH = DATA_DIR / "credentials.json"
CLAIMS_PATH = DATA_DIR / "claims.json"


class AppConfig:
    """Manages persistent application configuration, priority queues, credentials, and claim history."""

    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Platform toggles
        self.twitch_enabled: bool = True
        self.kick_enabled: bool = True

        # Automation toggles
        self.auto_claim: bool = True
        self.auto_switch: bool = True
        self.auto_claim_points: bool = True  # Twitch channel points
        self.enable_badges_emotes: bool = True
        self.prefer_manual_queue: bool = True  # Check manual channel queue before automatic campaign discovery

        # Priority Queue & Rules
        self.priority_mode: PriorityMode = PriorityMode.PRIORITY_FIRST
        self.twitch_priority_only: bool = False
        self.kick_priority_only: bool = True
        self.twitch_priority_games: list[str] = []
        self.kick_priority_games: list[str] = []
        self.excluded_games: list[str] = []

        # Manual Stream Queue (like KickDropsMiner queue + Twitch support)
        self.manual_queue: list[ManualQueueItem] = []

        # Intervals & Network
        self.inventory_refresh_minutes: int = 30
        self.channel_check_seconds: int = 60
        self.proxy: str = ""
        self.discord_webhook_url: str = ""

        # Credentials store
        self.credentials: dict[str, dict[str, Any]] = {
            "twitch": {
                "access_token": "",
                "user_id": "",
                "username": "",
                "device_id": "",
                "session_id": "",
            },
            "kick": {
                "session_token": "",
                "user_id": "",
                "username": "",
                "cookies": [],
            },
        }

        # Claim history
        self.claims: list[ClaimRecord] = []

        self.load()

    def load(self) -> None:
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.twitch_enabled = bool(data.get("twitch_enabled", True))
                self.kick_enabled = bool(data.get("kick_enabled", True))
                self.auto_claim = bool(data.get("auto_claim", True))
                self.auto_switch = bool(data.get("auto_switch", True))
                self.auto_claim_points = bool(data.get("auto_claim_points", True))
                self.enable_badges_emotes = bool(data.get("enable_badges_emotes", True))
                self.prefer_manual_queue = bool(data.get("prefer_manual_queue", True))
                mode_str = data.get("priority_mode", PriorityMode.PRIORITY_FIRST.value)
                try:
                    self.priority_mode = PriorityMode(mode_str)
                except ValueError:
                    self.priority_mode = PriorityMode.PRIORITY_FIRST
                self.twitch_priority_only = bool(data.get("twitch_priority_only", False))
                self.kick_priority_only = bool(data.get("kick_priority_only", True))
                self.twitch_priority_games = list(data.get("twitch_priority_games", []))
                self.kick_priority_games = list(data.get("kick_priority_games", []))
                self.excluded_games = list(data.get("excluded_games", []))
                self.inventory_refresh_minutes = max(5, int(data.get("inventory_refresh_minutes", 30)))
                self.channel_check_seconds = max(15, int(data.get("channel_check_seconds", 60)))
                self.proxy = str(data.get("proxy", ""))
                self.discord_webhook_url = str(data.get("discord_webhook_url", ""))

                raw_queue = data.get("manual_queue", [])
                self.manual_queue = []
                for item in raw_queue:
                    try:
                        self.manual_queue.append(ManualQueueItem(**item))
                    except Exception:
                        pass
            except Exception as e:
                print(f"Error loading config.json: {e}")

        if CREDENTIALS_PATH.exists():
            try:
                with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
                    creds = json.load(f)
                if "twitch" in creds and isinstance(creds["twitch"], dict):
                    self.credentials["twitch"].update(creds["twitch"])
                if "kick" in creds and isinstance(creds["kick"], dict):
                    self.credentials["kick"].update(creds["kick"])
            except Exception as e:
                print(f"Error loading credentials.json: {e}")

        if CLAIMS_PATH.exists():
            try:
                with open(CLAIMS_PATH, "r", encoding="utf-8") as f:
                    raw_claims = json.load(f)
                self.claims = [ClaimRecord(**c) for c in raw_claims[:200]]
            except Exception:
                self.claims = []

    def save(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "twitch_enabled": self.twitch_enabled,
            "kick_enabled": self.kick_enabled,
            "auto_claim": self.auto_claim,
            "auto_switch": self.auto_switch,
            "auto_claim_points": self.auto_claim_points,
            "enable_badges_emotes": self.enable_badges_emotes,
            "prefer_manual_queue": self.prefer_manual_queue,
            "priority_mode": self.priority_mode.value,
            "twitch_priority_only": self.twitch_priority_only,
            "kick_priority_only": self.kick_priority_only,
            "twitch_priority_games": self.twitch_priority_games,
            "kick_priority_games": self.kick_priority_games,
            "excluded_games": self.excluded_games,
            "manual_queue": [item.model_dump(mode="json") for item in self.manual_queue],
            "inventory_refresh_minutes": self.inventory_refresh_minutes,
            "channel_check_seconds": self.channel_check_seconds,
            "proxy": self.proxy,
            "discord_webhook_url": self.discord_webhook_url,
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def save_credentials(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(CREDENTIALS_PATH, "w", encoding="utf-8") as f:
            json.dump(self.credentials, f, indent=2)

    def record_claim(
        self,
        platform: Platform,
        campaign_name: str,
        game_name: str,
        reward_name: str,
        image_url: str = "",
    ) -> ClaimRecord:
        rec = ClaimRecord(
            id=str(uuid.uuid4()),
            platform=platform,
            campaign_name=campaign_name,
            game_name=game_name,
            reward_name=reward_name,
            image_url=image_url,
        )
        self.claims.insert(0, rec)
        self.claims = self.claims[:200]
        try:
            with open(CLAIMS_PATH, "w", encoding="utf-8") as f:
                json.dump([c.model_dump(mode="json") for c in self.claims], f, indent=2)
        except Exception:
            pass
        return rec

    def add_manual_queue_item(
        self,
        url: str,
        target_minutes: int = 120,
        campaign_id: Optional[str] = None,
        required_category_id: Optional[str] = None,
        game_name: str = "",
        alternative_channels: Optional[list[str]] = None,
    ) -> ManualQueueItem:
        url = url.strip()
        if "kick.com" in url.lower():
            platform = Platform.KICK
            username = url.rstrip("/").split("/")[-1].split("?")[0]
            clean_url = f"https://kick.com/{username}"
        elif "twitch.tv" in url.lower():
            platform = Platform.TWITCH
            username = url.rstrip("/").split("/")[-1].split("?")[0]
            clean_url = f"https://www.twitch.tv/{username}"
        else:
            # Default to Kick if just username or assume Twitch if prefixed
            username = url.strip("/").split("/")[0]
            platform = Platform.KICK
            clean_url = f"https://kick.com/{username}"

        # Check if already in queue
        for existing in self.manual_queue:
            if existing.url.lower() == clean_url.lower() and not existing.finished:
                existing.target_minutes = target_minutes
                self.save()
                return existing

        item = ManualQueueItem(
            id=str(uuid.uuid4())[:8],
            platform=platform,
            url=clean_url,
            username=username,
            target_minutes=max(0, int(target_minutes)),
            campaign_id=campaign_id,
            required_category_id=required_category_id,
            game_name=game_name,
            alternative_channels=alternative_channels or [],
        )
        self.manual_queue.append(item)
        self.save()
        return item

    def to_dict(self) -> dict[str, Any]:
        return {
            "twitch_enabled": self.twitch_enabled,
            "kick_enabled": self.kick_enabled,
            "auto_claim": self.auto_claim,
            "auto_switch": self.auto_switch,
            "auto_claim_points": self.auto_claim_points,
            "enable_badges_emotes": self.enable_badges_emotes,
            "prefer_manual_queue": self.prefer_manual_queue,
            "priority_mode": self.priority_mode.value,
            "twitch_priority_only": self.twitch_priority_only,
            "kick_priority_only": self.kick_priority_only,
            "twitch_priority_games": self.twitch_priority_games,
            "kick_priority_games": self.kick_priority_games,
            "excluded_games": self.excluded_games,
            "manual_queue": [item.model_dump(mode="json") for item in self.manual_queue],
            "inventory_refresh_minutes": self.inventory_refresh_minutes,
            "channel_check_seconds": self.channel_check_seconds,
            "proxy": self.proxy,
            "discord_webhook_url": self.discord_webhook_url,
        }
