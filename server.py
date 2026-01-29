import asyncio
from config import HOST, PORT, CLEANUP_INTERVAL, AOF_PATH
from store import Store
from protocol import parse_line, resp_ok, resp_err, resp_int, resp_bulk, resp_array, resp_frames, resp_list

from aof import AOF
from resp import read_frame, RespError

print("AOF PATH =>", AOF_PATH.resolve())

store = Store()


pubsub_lock = asyncio.Lock()
channels: dict[str, set[asyncio.StreamWriter]] = {}

aof = AOF(AOF_PATH)

async def handle_command(parts: list[str]) -> str:
    if not parts:
        return resp_err("empty command")

    cmd = parts[0].upper()

    try:
        if cmd == "PING":
            return resp_ok("PONG")

        if cmd == "SET":
            if len(parts) < 3:
                return resp_err("SET requires key value")
            key = parts[1]
            value = " ".join(parts[2:])

            evicted = await store.set(key, value)

            aof.append(f"SET {key} {value}")
            for k in evicted:
                aof.append(f"DEL {k}")

            return resp_ok()

        if cmd == "GET":
            if len(parts) != 2:
                return resp_err("GET requires key")
            v = await store.get(parts[1])
            return resp_bulk(v)

        if cmd == "DEL":
            if len(parts) != 2:
                return resp_err("DEL requires key")
            n = await store.delete(parts[1])
            if n == 1:
                aof.append(f"DEL {parts[1]}")
            return resp_int(n)

        if cmd == "EXISTS":
            if len(parts) != 2:
                return resp_err("EXISTS requires key")
            n = await store.exists(parts[1])
            return resp_int(n)

        if cmd == "INCR":
            if len(parts) != 2:
                return resp_err("INCR requires key")
            n = await store.incr(parts[1])
            aof.append(f"INCR {parts[1]}")
            return resp_int(n)

        if cmd == "EXPIRE":
            if len(parts) != 3:
                return resp_err("EXPIRE requires key seconds")
            key = parts[1]
            seconds = int(parts[2])
            n = await store.expire(key, seconds)
            if n == 1:
                aof.append(f"EXPIRE {key} {seconds}")
            return resp_int(n)

        if cmd == "TTL":
            if len(parts) != 2:
                return resp_err("TTL requires key")
            n = await store.ttl(parts[1])
            return resp_int(n)

        if cmd == "FLUSHALL":
            n = await store.clear()
            aof.append("FLUSHALL")
            return resp_ok()

        if cmd == "KEYS":
            ks = await store.keys()
            return resp_array(ks)

        if cmd == "HELP":
            return resp_bulk("Commands: PING, SET, GET, DEL, EXISTS, INCR, EXPIRE, TTL, KEYS, MULTI, EXEC, DISCARD")

        return resp_err(f"unknown command '{cmd}'")

    except ValueError as ve:
        return resp_err(str(ve))
    except Exception as e:
        return resp_err(f"internal error: {e}")

async def client_loop(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    in_multi = False
    queue: list[list[str]] = []
    subscribed: set[str] = set()

    try:
        while True:
            try:
                frame = await read_frame(reader)
            except EOFError:
                break
            except RespError as e:
                writer.write(resp_err(str(e)).encode("utf-8"))
                await writer.drain()
                continue

            if isinstance(frame, list):
                parts = [str(x) for x in frame if x is not None]
            else:
                parts = [str(frame)]

            if not parts:
                writer.write(resp_err("empty command").encode("utf-8"))
                await writer.drain()
                continue

            cmd = parts[0].upper()

            if cmd == "QUIT":
                writer.write(resp_ok("BYE").encode("utf-8"))
                await writer.drain()
                break

            if cmd == "SUBSCRIBE":
                if len(parts) < 2:
                    writer.write(resp_err("SUBSCRIBE requires channel").encode("utf-8"))
                    await writer.drain()
                    continue

                async with pubsub_lock:
                    for ch in parts[1:]:
                        channels.setdefault(ch, set()).add(writer)
                        subscribed.add(ch)
                        count = len(subscribed)
                        writer.write(resp_list(["subscribe", ch, count]).encode("utf-8"))
                    await writer.drain()
                continue

            if cmd == "UNSUBSCRIBE":
                targets = parts[1:] if len(parts) > 1 else list(subscribed)

                async with pubsub_lock:
                    for ch in targets:
                        if ch in subscribed:
                            subscribed.remove(ch)
                        if ch in channels:
                            channels[ch].discard(writer)
                            if not channels[ch]:
                                del channels[ch]
                        count = len(subscribed)
                        writer.write(resp_list(["unsubscribe", ch, count]).encode("utf-8"))
                    await writer.drain()
                continue

            if cmd == "PUBLISH":
                if len(parts) < 3:
                    writer.write(resp_err("PUBLISH requires channel message").encode("utf-8"))
                    await writer.drain()
                    continue

                ch = parts[1]
                msg = " ".join(parts[2:])

                async with pubsub_lock:
                    subs = list(channels.get(ch, set()))

                for w in subs:
                    try:
                        w.write(resp_list(["message", ch, msg]).encode("utf-8"))
                    except Exception:
                        pass

                for w in subs:
                    try:
                        await w.drain()
                    except Exception:
                        pass

                writer.write(resp_int(len(subs)).encode("utf-8"))
                await writer.drain()
                continue

            if cmd == "MULTI":
                in_multi = True
                queue.clear()
                writer.write(resp_ok().encode("utf-8"))
                await writer.drain()
                continue

            if cmd == "DISCARD":
                in_multi = False
                queue.clear()
                writer.write(resp_ok().encode("utf-8"))
                await writer.drain()
                continue

            if cmd == "EXEC":
                if not in_multi:
                    writer.write(resp_err("EXEC without MULTI").encode("utf-8"))
                    await writer.drain()
                    continue

                in_multi = False
                frames: list[str] = []

                for queued_parts in queue:
                    frames.append(await handle_command(queued_parts))

                queue.clear()
                resp = resp_frames(frames)
                writer.write(resp.encode("utf-8"))
                await writer.drain()
                continue

            if in_multi:
                queue.append(parts)
                writer.write(resp_ok("QUEUED").encode("utf-8"))
                await writer.drain()
                continue

            resp = await handle_command(parts)
            writer.write(resp.encode("utf-8"))
            await writer.drain()

    finally:
        async with pubsub_lock:
            for ch in list(subscribed):
                if ch in channels:
                    channels[ch].discard(writer)
                    if not channels[ch]:
                        del channels[ch]
            subscribed.clear()
        writer.close()
        await writer.wait_closed()

async def cleanup_task():
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        await store.cleanup()

async def replay_aof():
    for line in aof.replay_lines():
        parts = parse_line(line)
        cmd = parts[0].upper() if parts else ""

        if cmd == "SET" and len(parts) >= 3:
            await store.set(parts[1], " ".join(parts[2:]))
        elif cmd == "DEL" and len(parts) == 2:
            await store.delete(parts[1])
        elif cmd == "INCR" and len(parts) == 2:
            try:
                await store.incr(parts[1])
            except ValueError:
                pass
        elif cmd == "EXPIRE" and len(parts) == 3:
            try:
                await store.expire(parts[1], int(parts[2]))
            except ValueError:
                pass

        elif cmd == "FLUSHALL":
            await store.clear()


async def main():
    await replay_aof()
    server = await asyncio.start_server(client_loop, HOST, PORT)
    asyncio.create_task(cleanup_task())

    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"mini-redis listening on {addrs}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
