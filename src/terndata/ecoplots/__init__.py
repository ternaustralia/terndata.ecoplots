import threading
from .utils import _background_cache_loader

# Start the background cache loading thread at import time
_cache_thread = threading.Thread(
    target=_background_cache_loader,
    daemon=True
)
_cache_thread.start()

from .ecoplots import EcoPlots, AsyncEcoPlots
__all__ = [
    "EcoPlots",
    "AsyncEcoPlots",
]
