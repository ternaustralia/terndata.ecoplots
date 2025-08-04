import threading
from terndata.ecoplots.utils import background_cache_loader

# Start the background cache loading thread at import time
_cache_thread = threading.Thread(
    target=background_cache_loader,
    daemon=True
)
_cache_thread.start()

from terndata.ecoplots.ecoplots import EcoPlots
__all__ = ["EcoPlots"]
