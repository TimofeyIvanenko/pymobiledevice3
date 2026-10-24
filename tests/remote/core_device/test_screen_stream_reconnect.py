import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

from pymobiledevice3.remote.core_device import screen_stream
from pymobiledevice3.remote.core_device.screen_stream import ScreenStreamServer
from pymobiledevice3.remote.remote_service_discovery import RemoteServiceDiscoveryService


def test_closed_userspace_dialer_triggers_reconnection():
    assert screen_stream._is_tunnel_dead_error(ConnectionError("userspace dial plane is closed"))
    assert not screen_stream._is_tunnel_dead_error(ValueError("invalid response"))


async def test_watchdog_recovers_when_a_failed_restart_left_no_active_service(monkeypatch):
    server = ScreenStreamServer(RemoteServiceDiscoveryService(("fd00::1", 1)))
    server._subscribers[asyncio.Queue()] = screen_stream._SubState()
    restart = AsyncMock(side_effect=ConnectionError("userspace dial plane is closed"))
    monkeypatch.setattr(server, "_ensure_fresh_stream", restart)
    sleep = asyncio.sleep

    async def poll(interval):
        await sleep(0)

    monkeypatch.setattr(screen_stream.asyncio, "sleep", poll)
    task = asyncio.create_task(server._stall_watchdog())
    try:
        await asyncio.wait_for(server._reconnect_signal.wait(), timeout=1)
        restart.assert_awaited_once_with(force=True)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("userspace,cached", [(True, False), (True, True), (False, False)])
async def test_reconnect_uses_the_original_transport_and_device(monkeypatch, userspace, cached):
    dialer = AsyncMock() if userspace else None
    old_rsd = RemoteServiceDiscoveryService(("fd00::1", 1), open_connection=dialer)
    old_rsd.udid = "target-device"
    new_rsd = old_rsd if cached else RemoteServiceDiscoveryService(("fd00::2", 2), open_connection=dialer)
    close = AsyncMock()
    monkeypatch.setattr(old_rsd, "close", close)
    local = AsyncMock(side_effect=[ConnectionResetError(), new_rsd])
    daemon = AsyncMock(side_effect=[None, new_rsd])
    monkeypatch.setattr(screen_stream, "establish_userspace_rsd", local)
    monkeypatch.setattr(screen_stream, "get_tunneld_device_by_udid", daemon)
    server = ScreenStreamServer(old_rsd)
    server._reconnect_poll_interval = 0.001
    resumed = asyncio.Event()

    async def start_stream(*, force):
        assert force
        resumed.set()

    for method in ("_stop_active_stream", "_stop_audio_stream", "_stop_hid", "_stop_clipboard_monitor"):
        monkeypatch.setattr(server, method, AsyncMock())
    monkeypatch.setattr(server, "_ensure_fresh_stream", start_stream)
    server._reconnect_signal.set()
    task = asyncio.create_task(server._reconnect_loop())
    try:
        await asyncio.wait_for(resumed.wait(), timeout=1)
        assert server._rsd is new_rsd
        assert server._sender_ip == new_rsd.service.address[0]
        if userspace:
            assert local.await_count == 2
            local.assert_awaited_with(serial="target-device")
            daemon.assert_not_awaited()
        else:
            assert daemon.await_count == 2
            daemon.assert_awaited_with("target-device")
            local.assert_not_awaited()
        if cached:
            close.assert_not_awaited()
        else:
            close.assert_awaited_once()
    finally:
        task.cancel()
        await task
