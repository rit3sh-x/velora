"""Velora one-shot seed package.

Run AFTER consumer + producer are up:

    uv run python -m seed

Both seeders (prices, tweets) are idempotent and skip-if-fresh.
"""
