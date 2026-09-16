"""Dependency-free LP publication policy shared by generation and storage."""


def source_refresh_failed(source: dict) -> bool:
    """Partial coverage/attribution is legitimate; stale or failed refresh is not."""
    if not isinstance(source, dict):
        raise ValueError("candidate liquidity sources must be objects")
    errors = source.get("errors")
    return source.get("status") == "stale" or (isinstance(errors, list) and bool(errors))
