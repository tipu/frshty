import asyncio
import os
import threading

import core.terminal as terminal


class _Socket:
    def __init__(self):
        self.received = []
        self.threads = set()

    async def send_bytes(self, data):
        self.threads.add(threading.get_ident())
        self.received.append(data)


def test_pty_output_reaches_readers_on_the_loop_thread():
    read_fd, write_fd = os.pipe()
    ws = _Socket()
    entry = {"fd": read_fd, "scrollback": bytearray(), "readers": {ws}}

    async def run():
        loop = asyncio.get_running_loop()
        reader = threading.Thread(target=terminal._background_reader, args=(entry, loop))
        reader.start()
        os.write(write_fd, b"hello")
        os.close(write_fd)
        await asyncio.to_thread(reader.join, 5)
        return threading.get_ident(), reader.is_alive()

    loop_thread, alive = asyncio.run(run())
    os.close(read_fd)
    assert not alive
    assert b"".join(ws.received) == b"hello"
    assert bytes(entry["scrollback"]) == b"hello"
    assert ws.threads == {loop_thread}

