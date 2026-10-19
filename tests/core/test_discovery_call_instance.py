import asyncio
import time

from core import discovery


def test_slow_instance_is_cut_at_the_total_timeout():
    async def dribble(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
                     b"content-length: 3\r\n\r\n")
        for byte in (b"{", b" ", b"}"):
            await asyncio.sleep(0.4)
            writer.write(byte)
            await writer.drain()
        writer.close()

    async def run():
        server = await asyncio.start_server(dribble, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        async with server:
            started = time.monotonic()
            result = await discovery.call_instance(f"http://127.0.0.1:{port}", "GET",
                                                   "/api/status", timeout=0.6)
            return result, time.monotonic() - started

    result, elapsed = asyncio.run(run())
    assert result == {"error": "no answer within 0.6s"}
    assert elapsed < 1.0
