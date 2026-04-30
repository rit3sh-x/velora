"""Allow `uv run python -m seed`."""
from __future__ import annotations

import asyncio

from seed.run import main


if __name__ == "__main__":
    asyncio.run(main())
