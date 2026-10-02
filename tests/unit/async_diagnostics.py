import asyncio


def pending_task_locations():
    """Report await locations only; never serialize coroutine arguments."""
    rows = []
    for task in asyncio.all_tasks():
        if task is asyncio.current_task():
            continue
        locations = []
        current = task.get_coro()
        while current is not None:
            code = getattr(current, "cr_code", getattr(current, "gi_code", None))
            frame = getattr(current, "cr_frame", getattr(current, "gi_frame", None))
            if code is not None:
                locations.append(f"{code.co_name}:{frame.f_lineno if frame else '?'}")
            current = getattr(current, "cr_await", getattr(current, "gi_yieldfrom", None))
        rows.append(f"{task.get_name()}: {' -> '.join(locations)}")
    return "\n".join(sorted(rows))


