def parse_line(line: str) -> list[str]:

    parts = line.strip().split()
    return parts

def resp_ok(msg: str = "OK") -> str:
    return f"+{msg}\r\n"

def resp_err(msg: str) -> str:
    return f"-ERR {msg}\r\n"

def resp_int(n: int) -> str:
    return f":{n}\r\n"

def resp_bulk(s: str | None) -> str:

    if s is None:
        return "$-1\r\n"
    b = s.encode("utf-8")
    return f"${len(b)}\r\n{s}\r\n"

def resp_array(items: list[str]) -> str:
    out = [f"*{len(items)}\r\n"]
    for it in items:
        out.append(resp_bulk(it))
    return "".join(out)


def resp_frames(frames: list[str]) -> str:
    return f"*{len(frames)}\r\n" + "".join(frames)


def resp_any(x) -> str:
    if x is None:
        return "$-1\r\n"
    if isinstance(x, int):
        return resp_int(x)
    return resp_bulk(str(x))

def resp_list(items: list) -> str:
    return f"*{len(items)}\r\n" + "".join(resp_any(i) for i in items)

