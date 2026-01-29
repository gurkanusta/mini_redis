import asyncio

class RespError(Exception):
    pass

async def read_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise EOFError
    if not line.endswith(b"\r\n"):

        line = line.rstrip(b"\n") + b"\r\n"
    return line

async def read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    data = await reader.readexactly(n)
    return data

async def read_frame(reader: asyncio.StreamReader):

    first = await reader.readexactly(1)

    if first == b"*":
        line = await read_line(reader)
        count = int(line[:-2])
        if count == -1:
            return None
        arr = []
        for _ in range(count):
            arr.append(await read_frame(reader))
        return arr

    if first == b"$":
        line = await read_line(reader)
        length = int(line[:-2])
        if length == -1:
            return None
        data = await read_exact(reader, length + 2)  # + \r\n
        return data[:-2].decode("utf-8", errors="replace")

    if first == b"+":
        line = await read_line(reader)
        return line[:-2].decode("utf-8", errors="replace")

    if first == b":":
        line = await read_line(reader)
        return int(line[:-2])

    if first == b"-":
        line = await read_line(reader)
        msg = line[:-2].decode("utf-8", errors="replace")
        raise RespError(msg)

    raise RespError(f"Unknown RESP type byte: {first!r}")
