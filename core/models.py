from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class Platform(str, Enum):
    TWITCH = "twitch"
    KICK = "kick"


class PriorityMode(str, Enum):
    PRIORITY_FIRST = "priority_first"      # Priority list first, then ending soonest
    PRIORITY_ONLY = "priority_only"        # Only mine games in the priority list
    ENDING_SOONEST = "ending_soonest"      # Sort by campaign end time
    LOW_AVAILABILITY = "low_availability"  # Sort by least remaining window vs required time


class MinerStatus(str, Enum):
    IDLE = "idle"
    STARTING = "starting"
    AUTH_REQUIRED = "auth_required"
    FETCHING = "fetching"
    WATCHING = "watching"
    SWITCHING = "switching"
    PAUSED = "paused"
    ERROR = "error"


class DropReward(BaseModel):
    id: str
    name: str
    platform: Platform
    campaign_id: str
    campaign_name: str = ""
    game_name: str = ""
    image_url: str = ""
    required_minutes: int = 0
    current_minutes: int = 0
    is_claimed: bool = False
    can_claim: bool = False
    claim_id: Optional[str] = None
    precondition_drops: list[str] = Field(default_factory=list)
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None

    @property
    def progress(self) -> float:
        if self.is_claimed:
            return 1.0
        if self.required_minutes <= 0:
            return 0.0
        return min(1.0, max(0.0, self.current_minutes / self.required_minutes))

    @property
    def progress_percent(self) -> int:
        return int(round(self.progress * 100))

    @property
    def remaining_minutes(self) -> int:
        if self.is_claimed:
            return 0
        return max(0, self.required_minutes - self.current_minutes)


class ChannelInfo(BaseModel):
    id: str
    username: str
    display_name: str
    platform: Platform
    url: str
    is_live: bool = False
    viewers: int = 0
    game_name: str = ""
    game_id: str = ""
    game_slug: str = ""
    title: str = ""
    drops_enabled: bool = True
    acl_based: bool = False
    broadcast_id: Optional[str] = None
    vod_id: Optional[str] = None
    profile_picture: str = ""


class DropCampaign(BaseModel):
    id: str
    name: str
    platform: Platform
    game_id: str = ""
    game_name: str = "Unknown Game"
    game_slug: str = ""
    image_url: str = ""
    status: str = "active"  # active, upcoming, expired, completed
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    account_linked: bool = True
    account_link_url: str = ""
    is_global_drop: bool = True
    allowed_channels: list[ChannelInfo] = Field(default_factory=list)
    drops: list[DropReward] = Field(default_factory=list)
    progress_units: int = 0

    @property
    def total_drops(self) -> int:
        return len(self.drops)

    @property
    def claimed_drops(self) -> int:
        return sum(1 for d in self.drops if d.is_claimed)

    @property
    def finished(self) -> bool:
        return bool(self.drops) and all(d.is_claimed or d.required_minutes <= 0 for d in self.drops)

    @property
    def is_active_time(self) -> bool:
        now = datetime.now(timezone.utc)
        if self.starts_at and now < self.starts_at:
            return False
        if self.ends_at and now >= self.ends_at:
            return False
        return self.status.lower() not in ("expired", "completed", "claimed")

    @property
    def overall_progress(self) -> float:
        if not self.drops:
            return 0.0
        return sum(d.progress for d in self.drops) / len(self.drops)

    @property
    def active_drop(self) -> Optional[DropReward]:
        unclaimed = [d for d in self.drops if not d.is_claimed and d.required_minutes > 0]
        if not unclaimed:
            return None
        unclaimed.sort(
            key=lambda d: (
                0 if d.current_minutes > 0 else 1,
                d.remaining_minutes,
                d.required_minutes,
            )
        )
        return unclaimed[0]

    @property
    def remaining_minutes(self) -> int:
        if not self.drops:
            return 0
        return max((d.remaining_minutes for d in self.drops if not d.is_claimed), default=0)

    def to_summary_dict(self) -> dict[str, Any]:
        active = self.active_drop
        return {
            "id": self.id,
            "name": self.name,
            "platform": self.platform.value,
            "game_id": self.game_id,
            "game_name": self.game_name,
            "game_slug": self.game_slug,
            "image_url": self.image_url,
            "status": "completed" if self.finished else self.status,
            "starts_at": self.starts_at.isoformat() if self.starts_at else None,
            "ends_at": self.ends_at.isoformat() if self.ends_at else None,
            "account_linked": self.account_linked,
            "account_link_url": self.account_link_url,
            "is_global_drop": self.is_global_drop,
            "total_drops": self.total_drops,
            "claimed_drops": self.claimed_drops,
            "finished": self.finished,
            "overall_progress": round(self.overall_progress * 100, 1),
            "remaining_minutes": self.remaining_minutes,
            "active_drop": {
                "id": active.id,
                "name": active.name,
                "current_minutes": active.current_minutes,
                "required_minutes": active.required_minutes,
                "progress_percent": active.progress_percent,
            } if active else None,
            "allowed_channels": [
                {
                    "username": ch.username,
                    "display_name": ch.display_name,
                    "url": ch.url,
                    "is_live": ch.is_live,
                    "viewers": ch.viewers,
                }
                for ch in self.allowed_channels[:25]
            ],
            "drops": [
                {
                    "id": d.id,
                    "name": d.name,
                    "image_url": d.image_url,
                    "required_minutes": d.required_minutes,
                    "current_minutes": d.current_minutes,
                    "progress_percent": d.progress_percent,
                    "is_claimed": d.is_claimed,
                    "can_claim": d.can_claim,
                    "claim_id": d.claim_id,
                }
                for d in self.drops
            ],
        }


class ManualQueueItem(BaseModel):
    id: str
    platform: Platform
    url: str
    username: str
    target_minutes: int = 120  # 0 = infinite
    elapsed_seconds: int = 0
    campaign_id: Optional[str] = None
    required_category_id: Optional[str] = None
    game_name: str = ""
    finished: bool = False
    is_live: bool = False
    alternative_channels: list[str] = Field(default_factory=list)


class ClaimRecord(BaseModel):
    id: str
    platform: Platform
    campaign_name: str
    game_name: str
    reward_name: str
    image_url: str = ""
    claimed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PlatformMinerState(BaseModel):
    platform: Platform
    enabled: bool = True
    authenticated: bool = False
    username: str = ""
    user_id: str = ""
    status: MinerStatus = MinerStatus.IDLE
    status_message: str = "Idle"
    watching_channel: Optional[ChannelInfo] = None
    active_campaign: Optional[dict[str, Any]] = None
    active_drop: Optional[dict[str, Any]] = None
    watch_method: str = "streamless"
    last_watch_ping: Optional[str] = None
    points_claimed_session: int = 0
    drops_claimed_session: int = 0
    auth_flow: Optional[dict[str, Any]] = None  # Pending device code / TV link info
