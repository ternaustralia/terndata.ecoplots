"""EcoPlots package.

Exposes the public clients and (best-effort) starts a background cache loader
on import. Import side effects are lightweight and safe to ignore if the
loader is unavailable.
"""

import threading

from ._utils import _background_cache_loader
from .ecoplots import AsyncEcoPlots, EcoPlots

__all__ = ["EcoPlots", "AsyncEcoPlots"]

# Start the background cache loading thread at import time (best-effort).
_cache_thread = threading.Thread(target=_background_cache_loader, daemon=True)
_cache_thread.start()
