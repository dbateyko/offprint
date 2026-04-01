"""Integration module for persistent cooldowns with existing adapters.

This module provides functions to sync the in-memory WAF state in
DigitalCommonsBaseAdapter with the persistent CooldownManager.

Usage:
    from offprint.cooldown_integration import (
        sync_cooldowns_to_adapter,
        sync_cooldowns_from_adapter,
        wrap_adapter_with_persistence,
    )

    # Before running a scrape, load persistent cooldowns into adapter
    sync_cooldowns_to_adapter(adapter_class)

    # After running, persist adapter cooldowns
    sync_cooldowns_from_adapter(adapter_class)

    # Or use context manager for automatic sync
    with wrap_adapter_with_persistence(adapter_class):
        # run scraper...
        pass
"""
from __future__ import annotations

import atexit
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, TYPE_CHECKING

from .cooldown_state import CooldownManager, get_cooldown_manager

if TYPE_CHECKING:
    pass


def sync_cooldowns_to_adapter(
    adapter_class: type,
    cooldowns: Optional[CooldownManager] = None,
) -> int:
    """Load persistent cooldowns into adapter's in-memory state.

    Args:
        adapter_class: The DigitalCommonsBaseAdapter class
        cooldowns: Optional cooldown manager (uses global singleton if None)

    Returns:
        Number of cooldowns loaded
    """
    if cooldowns is None:
        cooldowns = get_cooldown_manager()

    now = time.time()
    loaded = 0

    with adapter_class._waf_state_lock:
        for key in cooldowns.list_keys():
            entry = cooldowns.get_entry(key)
            if not entry:
                continue

            # Only load if still in cooldown
            if entry.cooldown_until > now:
                adapter_class._waf_state[key] = {
                    "streak": float(entry.failure_streak),
                    "open_until": entry.cooldown_until,
                }
                loaded += 1
            elif entry.failure_streak > 0:
                # Load streak even if cooldown expired
                adapter_class._waf_state[key] = {
                    "streak": float(entry.failure_streak),
                    "open_until": 0.0,
                }

    return loaded


def sync_cooldowns_from_adapter(
    adapter_class: type,
    cooldowns: Optional[CooldownManager] = None,
    save: bool = True,
) -> int:
    """Persist adapter's in-memory cooldowns to storage.

    Args:
        adapter_class: The DigitalCommonsBaseAdapter class
        cooldowns: Optional cooldown manager (uses global singleton if None)
        save: Whether to save immediately (default True)

    Returns:
        Number of cooldowns persisted
    """
    if cooldowns is None:
        cooldowns = get_cooldown_manager()

    persisted = 0

    with adapter_class._waf_state_lock:
        for key, state in adapter_class._waf_state.items():
            streak = int(state.get("streak", 0))
            open_until = float(state.get("open_until", 0.0))

            if streak > 0 or open_until > 0:
                entry = cooldowns._get_or_create_entry(key)
                entry.failure_streak = streak
                entry.cooldown_until = open_until
                cooldowns._dirty = True
                persisted += 1

    if save and persisted > 0:
        cooldowns.save()

    return persisted


@contextmanager
def wrap_adapter_with_persistence(
    adapter_class: type,
    cooldowns: Optional[CooldownManager] = None,
    sync_interval_seconds: float = 60.0,
):
    """Context manager that syncs cooldowns before/after operations.

    Args:
        adapter_class: The DigitalCommonsBaseAdapter class
        cooldowns: Optional cooldown manager (uses global singleton if None)
        sync_interval_seconds: How often to sync during operation

    Yields:
        The cooldown manager being used
    """
    if cooldowns is None:
        cooldowns = get_cooldown_manager()

    # Load existing cooldowns
    loaded = sync_cooldowns_to_adapter(adapter_class, cooldowns)
    if loaded > 0:
        print(f"Loaded {loaded} persistent cooldowns into adapter")

    # Background sync thread
    stop_event = threading.Event()
    last_sync = time.time()

    def background_sync():
        nonlocal last_sync
        while not stop_event.wait(sync_interval_seconds):
            sync_cooldowns_from_adapter(adapter_class, cooldowns, save=True)
            last_sync = time.time()

    sync_thread = threading.Thread(target=background_sync, daemon=True)
    sync_thread.start()

    try:
        yield cooldowns
    finally:
        # Stop background sync
        stop_event.set()
        sync_thread.join(timeout=2.0)

        # Final sync
        persisted = sync_cooldowns_from_adapter(adapter_class, cooldowns, save=True)
        if persisted > 0:
            print(f"Persisted {persisted} cooldowns from adapter")


def install_persistence_hooks(
    adapter_class: type,
    cooldowns: Optional[CooldownManager] = None,
) -> None:
    """Install atexit hooks to persist cooldowns on exit.

    Args:
        adapter_class: The DigitalCommonsBaseAdapter class
        cooldowns: Optional cooldown manager (uses global singleton if None)
    """
    if cooldowns is None:
        cooldowns = get_cooldown_manager()

    # Load existing cooldowns
    loaded = sync_cooldowns_to_adapter(adapter_class, cooldowns)
    if loaded > 0:
        print(f"Loaded {loaded} persistent cooldowns")

    # Register cleanup
    def save_on_exit():
        persisted = sync_cooldowns_from_adapter(adapter_class, cooldowns, save=True)
        if persisted > 0:
            print(f"Saved {persisted} cooldowns on exit")

    atexit.register(save_on_exit)


def get_combined_cooldown_status(
    adapter_class: type,
    cooldowns: Optional[CooldownManager] = None,
) -> Dict[str, Any]:
    """Get combined cooldown status from adapter and persistent storage.

    Args:
        adapter_class: The DigitalCommonsBaseAdapter class
        cooldowns: Optional cooldown manager (uses global singleton if None)

    Returns:
        Dict with combined status
    """
    if cooldowns is None:
        cooldowns = get_cooldown_manager()

    now = time.time()
    in_memory_active = 0
    persistent_active = 0
    all_keys = set()

    # Count in-memory
    with adapter_class._waf_state_lock:
        for key, state in adapter_class._waf_state.items():
            all_keys.add(key)
            open_until = float(state.get("open_until", 0.0))
            if open_until > now:
                in_memory_active += 1

    # Count persistent
    for key in cooldowns.list_keys():
        all_keys.add(key)
        entry = cooldowns.get_entry(key)
        if entry and entry.cooldown_until > now:
            persistent_active += 1

    return {
        "in_memory_active": in_memory_active,
        "persistent_active": persistent_active,
        "total_tracked": len(all_keys),
        "keys": sorted(all_keys),
    }
