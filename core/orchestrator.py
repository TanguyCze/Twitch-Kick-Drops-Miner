from __future__ import annotations

import asyncio
from typing import Any, Optional

import aiohttp

from .config import AppConfig
from .events import EventBus
from .kick_miner import KickMiner
from .models import DropCampaign, Platform, PriorityMode
from .twitch_miner import TwitchMiner


class MinerOrchestrator:
    """Coordinates both TwitchMiner and KickMiner, shared state, and notifications."""

    def __init__(self) -> None:
        self.config = AppConfig()
        self.events = EventBus()
        self.twitch = TwitchMiner(self.config, self.events)
        self.kick = KickMiner(self.config, self.events)
        self.events.set_webhook_callback(self._send_discord_webhook)

    async def start(self) -> None:
        self.events.log("Starting Unified Twitch & Kick Drops Miner...", platform="system")
        await asyncio.gather(
            self.twitch.start(),
            self.kick.start(),
        )

    async def stop(self) -> None:
        self.events.log("Stopping miners...", platform="system")
        await asyncio.gather(
            self.twitch.stop(),
            self.kick.stop(),
            return_exceptions=True,
        )

    async def _send_discord_webhook(self, claim_data: dict[str, Any]) -> None:
        webhook_url = self.config.discord_webhook_url.strip()
        if not webhook_url:
            return
        platform_str = str(claim_data.get("platform", "")).upper()
        color = 0x9146FF if platform_str == "TWITCH" else 0x53FC18
        embed: dict[str, Any] = {
            "title": f"🎁 {platform_str} Drop Claimed!",
            "description": (
                f"**Reward:** {claim_data.get('reward_name')}\n"
                f"**Game:** {claim_data.get('game_name')}\n"
                f"**Campaign:** {claim_data.get('campaign_name')}"
            ),
            "color": color,
            "timestamp": claim_data.get("claimed_at"),
        }
        if claim_data.get("image_url"):
            embed["thumbnail"] = {"url": claim_data["image_url"]}
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(webhook_url, json={"embeds": [embed]})
        except Exception as e:
            self.events.log(f"Discord webhook failed: {e}", platform="system", level="warn")

    def get_full_status(self) -> dict[str, Any]:
        return {
            "twitch": self.twitch.state.model_dump(mode="json"),
            "kick": self.kick.state.model_dump(mode="json"),
            "config": self.config.to_dict(),
            "claims": [c.model_dump(mode="json") for c in self.config.claims[:50]],
            "logs": self.events.get_logs()[-100:],
            "stats": {
                "twitch_campaigns": len(self.twitch.campaigns),
                "kick_campaigns": len(self.kick.campaigns),
                "total_claimed_history": len(self.config.claims),
            },
        }

    def get_all_campaigns(self, platform_filter: Optional[str] = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if platform_filter in (None, "all", "twitch"):
            out.extend(c.to_summary_dict() for c in self.twitch.campaigns)
        if platform_filter in (None, "all", "kick"):
            out.extend(c.to_summary_dict() for c in self.kick.campaigns)
        return out

    def get_available_games(self) -> dict[str, list[str]]:
        t_games = sorted({c.game_name for c in self.twitch.campaigns if c.game_name})
        k_games = sorted({c.game_name for c in self.kick.campaigns if c.game_name})
        return {"twitch": t_games, "kick": k_games}

    async def queue_campaign(self, platform: str, campaign_id: str) -> dict[str, Any]:
        """Adds a campaign's live or eligible channels to the manual queue and prioritizes its game."""
        camp: Optional[DropCampaign] = None
        if platform == "twitch":
            camp = next((c for c in self.twitch.campaigns if c.id == campaign_id), None)
        elif platform == "kick":
            camp = next((c for c in self.kick.campaigns if c.id == campaign_id), None)

        if not camp:
            raise ValueError(f"Campaign {campaign_id} not found on {platform}")

        # Add game to top of priority list if not already present
        prio_list = (
            self.config.twitch_priority_games
            if platform == "twitch"
            else self.config.kick_priority_games
        )
        if camp.game_name and camp.game_name not in prio_list:
            prio_list.insert(0, camp.game_name)

        # Also find candidate channels and add top channel + alternatives to manual queue
        if platform == "kick":
            cands = await self.kick.find_candidates_for_campaign(camp)
            if not cands and camp.allowed_channels:
                cands = camp.allowed_channels
        else:
            cands = await self.twitch.find_candidates_for_campaign(camp)
            if not cands and camp.allowed_channels:
                cands = camp.allowed_channels

        added_item = None
        if cands:
            top = cands[0]
            alts = [c.url for c in cands[1:15]]
            target_mins = camp.remaining_minutes or 120
            added_item = self.config.add_manual_queue_item(
                url=top.url,
                target_minutes=target_mins,
                campaign_id=camp.id,
                required_category_id=camp.game_id or None,
                game_name=camp.game_name,
                alternative_channels=alts,
            )

        self.config.save()
        if platform == "twitch":
            self.twitch.wake()
        else:
            self.kick.wake()

        return {
            "ok": True,
            "game_prioritized": camp.game_name,
            "queued_item": added_item.model_dump(mode="json") if added_item else None,
        }
