def replace_builder(builder, segment: str) -> None:
    """Append a segment string to the builder (list or StringIO-like).
    """
    try:
        if hasattr(builder, 'append'):
            builder.append(segment)
        elif hasattr(builder, 'write'):
            builder.write(segment)
    except Exception:
        pass
