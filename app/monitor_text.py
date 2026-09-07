"""Plain-text rendering shared by monitor intake and case cards."""

def safe_monitor_text(value: object, *, limit: int) -> str:
    text = str(value or "")
    # The source is untrusted monitoring text. Keep only plain, single-line text
    # and strip markdown/control delimiters so future consumers don't inherit a
    # prompt-like or rich-text channel from webhook payloads.
    cleaned = []
    blocked = set("`<>[]{}")
    for ch in text:
        if ch in blocked or ord(ch) < 32:
            cleaned.append(" ")
        else:
            cleaned.append(ch)
    rendered = " ".join("".join(cleaned).split())
    return (rendered or "—")[:limit]

