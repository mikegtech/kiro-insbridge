def find_next_var(sequence, current_index: int) -> int:
    """Stub for finding the next variable index in algorithm_seq.dependency_vars.
    Returns the smallest index > current_index, or current_index if none.
    """
    try:
        indices = []
        for dep in getattr(sequence, 'dependency_vars', []):
            try:
                idx = int(dep.index)
                indices.append(idx)
            except (TypeError, ValueError):
                continue
        indices.sort()
        for idx in indices:
            if idx > current_index:
                return idx
    except Exception:
        pass
    return current_index
