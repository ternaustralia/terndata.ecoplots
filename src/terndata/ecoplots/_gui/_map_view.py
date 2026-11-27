"""Small, synchronous view updates for standard ipyleaflet maps."""

import math


def _set_map_bounds(map_widget, bounds):
    """Fit geographic bounds with one view update and no frontend extension.

    Args:
        map_widget: The ipyleaflet map to update.
        bounds: Latitude/longitude bounds as [[south, west], [north, east]].
    """
    (south, west), (north, east) = bounds

    def mercator_y(latitude):
        latitude = max(-85.051129, min(85.051129, latitude))
        return math.asinh(math.tan(math.radians(latitude)))

    bottom, top = mercator_y(south), mercator_y(north)
    width, height = 800, 500
    if map_widget.pixel_bounds:
        (left, upper), (right, lower) = map_widget.pixel_bounds
        if right > left and lower > upper:
            width, height = right - left, lower - upper
    x_fraction = max(abs(east - west) / 360, 1e-12)
    y_fraction = max(abs(top - bottom) / (2 * math.pi), 1e-12)
    zoom = math.floor(
        min(
            math.log2(max(width - 40, 1) / (256 * x_fraction)),
            math.log2(max(height - 40, 1) / (256 * y_fraction)),
        )
    )
    with map_widget.hold_sync():
        map_widget.center = (
            math.degrees(math.atan(math.sinh((bottom + top) / 2))),
            (west + east) / 2,
        )
        map_widget.zoom = max(map_widget.min_zoom, min(zoom, map_widget.max_zoom))
