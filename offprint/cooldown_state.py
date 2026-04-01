"""Persistent domain cooldown state for WAF circuit breakers.

This module provides persistent storage for domain cooldown states, allowing:
- Circuit breaker state to survive process restarts
- Retry scripts to respect cooldowns from previous runs
- Unified cooldown management across all download tools

State is stored in JSON format at a configurable path (default: runs/cooldowns.json).

Usage:
    from offprint.cooldown_state import CooldownManager

    # Create manager (loads existing state)
    cooldowns = CooldownManager()

    # Check if a domain is in cooldown
    is_blocked, remaining_seconds = cooldowns.check_cooldown("example.edu|lawreview")
    if is_blocked:
        print(f"Domain in cooldown for {remaining_seconds:.0f}s more")

    # Record a failure (may open circuit)
    circuit_opened = cooldowns.record_failure(
        "example.edu|lawreview",
        fail_threshold=3,
        cooldown_seconds=900,
    )

    # Record success (resets failure streak)
    cooldowns.record_success("example.edu|lawreview")

    # Save state explicitly (auto-saved on changes)
    cooldowns.save()
"""
from __future__ import annotations

import atexit
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DomainCooldownEntry:
    """State for a single domain/scope."""
    key: str  # domain or domain|scope
    failure_streak: int = 0
    cooldown_until: float = 0.0  # Unix timestamp
    last_failure_at: str = ""
    last_success_at: str = ""
    total_failures: int = 0
    total_successes: int = 0
    error_types: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "failure_streak": self.failure_streak,
            "cooldown_until": self.cooldown_until,
            "last_failure_at": self.last_failure_at,
            "last_success_at": self.last_success_at,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "error_types": self.error_types,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DomainCooldownEntry":
        return cls(
            key=data.get("key", ""),
            failure_streak=int(data.get("failure_streak", 0)),
            cooldown_until=float(data.get("cooldown_until", 0.0)),
            last_failure_at=data.get("last_failure_at", ""),
            last_success_at=data.get("last_success_at", ""),
            total_failures=int(data.get("total_failures", 0)),
            total_successes=int(data.get("total_successes", 0)),
            error_types=dict(data.get("error_types", {})),
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CooldownManager:
    """Manages persistent domain cooldown state.

    Thread-safe implementation with automatic persistence.
    """

    DEFAULT_PATH = "runs/cooldowns.json"

    def __init__(
        self,
        state_path: Optional[str] = None,
        auto_save: bool = True,
        save_interval_seconds: float = 30.0,
    ):
        """Initialize cooldown manager.

        Args:
            state_path: Path to state file (default: runs/cooldowns.json)
            auto_save: Whether to auto-save on changes (default: True)
            save_interval_seconds: Minimum time between auto-saves (default: 30s)
        """
        self._state_path = Path(state_path or self.DEFAULT_PATH)
        self._auto_save = auto_save
        self._save_interval = save_interval_seconds
        self._last_save_time = 0.0
        self._dirty = False

        self._lock = threading.RLock()
        self._entries: Dict[str, DomainCooldownEntry] = {}

        # Load existing state
        self._load()

        # Register save on exit
        atexit.register(self._save_if_dirty)

    def _load(self) -> None:
        """Load state from file."""
        if not self._state_path.exists():
            return

        try:
            with self._state_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            with self._lock:
                self._entries.clear()
                for key, entry_data in data.get("entries", {}).items():
                    self._entries[key] = DomainCooldownEntry.from_dict(entry_data)

        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to load cooldown state from {self._state_path}: {e}")

    def save(self) -> None:
        """Save state to file."""
        with self._lock:
            data = {
                "version": 1,
                "saved_at": _utc_now_iso(),
                "entries": {
                    key: entry.to_dict()
                    for key, entry in self._entries.items()
                },
            }

        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            # Write atomically via temp file
            temp_path = self._state_path.with_suffix(".json.tmp")
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
            temp_path.replace(self._state_path)
            self._last_save_time = time.time()
            self._dirty = False
        except IOError as e:
            print(f"Warning: Failed to save cooldown state to {self._state_path}: {e}")

    def _save_if_dirty(self) -> None:
        """Save if there are unsaved changes."""
        if self._dirty:
            self.save()

    def _maybe_auto_save(self) -> None:
        """Auto-save if enabled and enough time has passed."""
        if not self._auto_save or not self._dirty:
            return
        if time.time() - self._last_save_time >= self._save_interval:
            self.save()

    def _get_or_create_entry(self, key: str) -> DomainCooldownEntry:
        """Get or create entry for a key."""
        with self._lock:
            if key not in self._entries:
                self._entries[key] = DomainCooldownEntry(key=key)
            return self._entries[key]

    def check_cooldown(self, key: str) -> Tuple[bool, float]:
        """Check if a domain/scope is in cooldown.

        Args:
            key: Domain or domain|scope key

        Returns:
            (is_in_cooldown, remaining_seconds)
        """
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return False, 0.0

            if entry.cooldown_until > now:
                return True, entry.cooldown_until - now

            # Cooldown expired - reset it
            if entry.cooldown_until > 0:
                entry.cooldown_until = 0.0
                entry.failure_streak = 0
                self._dirty = True
                self._maybe_auto_save()

            return False, 0.0

    def check_cooldown_closed(self, key: str) -> Tuple[bool, float, bool]:
        """Check cooldown with indication if it just closed.

        Args:
            key: Domain or domain|scope key

        Returns:
            (is_in_cooldown, remaining_seconds, just_closed)
        """
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return False, 0.0, False

            if entry.cooldown_until > now:
                return True, entry.cooldown_until - now, False

            # Cooldown expired - reset it
            if entry.cooldown_until > 0:
                entry.cooldown_until = 0.0
                entry.failure_streak = 0
                self._dirty = True
                self._maybe_auto_save()
                return False, 0.0, True

            return False, 0.0, False

    def record_failure(
        self,
        key: str,
        fail_threshold: int = 3,
        cooldown_seconds: int = 900,
        error_type: str = "",
    ) -> Tuple[bool, int]:
        """Record a failure and potentially open circuit.

        Args:
            key: Domain or domain|scope key
            fail_threshold: Number of failures before opening circuit
            cooldown_seconds: How long to keep circuit open
            error_type: Type of error (for statistics)

        Returns:
            (circuit_opened, current_streak)
        """
        now = time.time()
        with self._lock:
            entry = self._get_or_create_entry(key)
            entry.failure_streak += 1
            entry.total_failures += 1
            entry.last_failure_at = _utc_now_iso()

            if error_type:
                entry.error_types[error_type] = entry.error_types.get(error_type, 0) + 1

            opened = False
            if (
                fail_threshold > 0
                and entry.failure_streak >= fail_threshold
                and cooldown_seconds > 0
            ):
                entry.cooldown_until = now + float(cooldown_seconds)
                opened = True

            self._dirty = True
            self._maybe_auto_save()

            return opened, entry.failure_streak

    def record_success(self, key: str) -> None:
        """Record a success (resets failure streak).

        Args:
            key: Domain or domain|scope key
        """
        with self._lock:
            entry = self._get_or_create_entry(key)
            entry.failure_streak = 0
            entry.cooldown_until = 0.0
            entry.total_successes += 1
            entry.last_success_at = _utc_now_iso()
            self._dirty = True
            self._maybe_auto_save()

    def get_active_cooldowns(self) -> List[Tuple[str, float]]:
        """Get all domains currently in cooldown.

        Returns:
            List of (key, remaining_seconds) tuples
        """
        now = time.time()
        active = []
        with self._lock:
            for key, entry in self._entries.items():
                if entry.cooldown_until > now:
                    active.append((key, entry.cooldown_until - now))
        return sorted(active, key=lambda x: x[1], reverse=True)

    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregate statistics.

        Returns:
            Dictionary with statistics
        """
        now = time.time()
        with self._lock:
            total_entries = len(self._entries)
            active_cooldowns = sum(
                1 for e in self._entries.values() if e.cooldown_until > now
            )
            total_failures = sum(e.total_failures for e in self._entries.values())
            total_successes = sum(e.total_successes for e in self._entries.values())

            error_type_totals: Dict[str, int] = {}
            for entry in self._entries.values():
                for et, count in entry.error_types.items():
                    error_type_totals[et] = error_type_totals.get(et, 0) + count

            return {
                "total_domains": total_entries,
                "active_cooldowns": active_cooldowns,
                "total_failures": total_failures,
                "total_successes": total_successes,
                "error_type_breakdown": error_type_totals,
            }

    def clear_cooldown(self, key: str) -> bool:
        """Manually clear cooldown for a domain.

        Args:
            key: Domain or domain|scope key

        Returns:
            True if cooldown was cleared, False if not in cooldown
        """
        with self._lock:
            entry = self._entries.get(key)
            if not entry or entry.cooldown_until <= 0:
                return False

            entry.cooldown_until = 0.0
            entry.failure_streak = 0
            self._dirty = True
            self._maybe_auto_save()
            return True

    def clear_all_cooldowns(self) -> int:
        """Clear all active cooldowns.

        Returns:
            Number of cooldowns cleared
        """
        now = time.time()
        cleared = 0
        with self._lock:
            for entry in self._entries.values():
                if entry.cooldown_until > now:
                    entry.cooldown_until = 0.0
                    entry.failure_streak = 0
                    cleared += 1
            if cleared > 0:
                self._dirty = True
                self.save()
        return cleared

    def get_entry(self, key: str) -> Optional[DomainCooldownEntry]:
        """Get entry for a key (for inspection).

        Args:
            key: Domain or domain|scope key

        Returns:
            Entry or None
        """
        with self._lock:
            return self._entries.get(key)

    def list_keys(self, pattern: str = "") -> List[str]:
        """List all keys, optionally filtered by pattern.

        Args:
            pattern: Optional substring to filter by

        Returns:
            List of matching keys
        """
        with self._lock:
            if not pattern:
                return sorted(self._entries.keys())
            return sorted(k for k in self._entries.keys() if pattern in k)


# Global singleton instance
_global_manager: Optional[CooldownManager] = None
_global_manager_lock = threading.Lock()


def get_cooldown_manager(state_path: Optional[str] = None) -> CooldownManager:
    """Get the global cooldown manager singleton.

    Args:
        state_path: Optional custom state path (only used on first call)

    Returns:
        Global CooldownManager instance
    """
    global _global_manager
    with _global_manager_lock:
        if _global_manager is None:
            _global_manager = CooldownManager(state_path=state_path)
        return _global_manager


def reset_cooldown_manager() -> None:
    """Reset the global cooldown manager (for testing)."""
    global _global_manager
    with _global_manager_lock:
        if _global_manager is not None:
            _global_manager._save_if_dirty()
        _global_manager = None


# CLI interface for manual management
def main() -> int:
    """CLI for managing cooldown state."""
    import argparse

    parser = argparse.ArgumentParser(description="Manage domain cooldown state")
    parser.add_argument(
        "--state-file",
        default=CooldownManager.DEFAULT_PATH,
        help="Path to state file",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # list command
    list_parser = subparsers.add_parser("list", help="List cooldowns")
    list_parser.add_argument("--active-only", action="store_true", help="Only show active")
    list_parser.add_argument("--pattern", default="", help="Filter by pattern")

    # clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cooldown")
    clear_parser.add_argument("key", nargs="?", help="Domain key to clear (or --all)")
    clear_parser.add_argument("--all", action="store_true", help="Clear all cooldowns")

    # stats command
    subparsers.add_parser("stats", help="Show statistics")

    # show command
    show_parser = subparsers.add_parser("show", help="Show entry details")
    show_parser.add_argument("key", help="Domain key to show")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    manager = CooldownManager(state_path=args.state_file, auto_save=False)

    if args.command == "list":
        keys = manager.list_keys(args.pattern)
        now = time.time()
        for key in keys:
            entry = manager.get_entry(key)
            if not entry:
                continue
            if args.active_only and entry.cooldown_until <= now:
                continue
            status = ""
            if entry.cooldown_until > now:
                remaining = entry.cooldown_until - now
                status = f" [COOLDOWN: {remaining:.0f}s remaining]"
            print(f"{key}: streak={entry.failure_streak}, total={entry.total_failures}{status}")
        return 0

    if args.command == "clear":
        if args.all:
            cleared = manager.clear_all_cooldowns()
            print(f"Cleared {cleared} cooldowns")
        elif args.key:
            if manager.clear_cooldown(args.key):
                manager.save()
                print(f"Cleared cooldown for {args.key}")
            else:
                print(f"No active cooldown for {args.key}")
        else:
            print("Specify --all or a key to clear")
            return 1
        return 0

    if args.command == "stats":
        stats = manager.get_statistics()
        print(f"Total domains tracked: {stats['total_domains']}")
        print(f"Active cooldowns: {stats['active_cooldowns']}")
        print(f"Total failures: {stats['total_failures']}")
        print(f"Total successes: {stats['total_successes']}")
        if stats["error_type_breakdown"]:
            print("\nError types:")
            for et, count in sorted(
                stats["error_type_breakdown"].items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {et}: {count}")
        return 0

    if args.command == "show":
        entry = manager.get_entry(args.key)
        if not entry:
            print(f"No entry for {args.key}")
            return 1
        print(json.dumps(entry.to_dict(), indent=2))
        return 0

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
