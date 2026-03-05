"""Analytics namespace.

This package intentionally avoids eager imports.

Some modules (e.g. changepoint detection) may rely on optional / heavy
dependencies. Importing them here would make *any* import of
`nps_lens.analytics` slower and more fragile.

Import concrete modules/functions instead:

    from nps_lens.analytics.drivers import driver_table
"""

__all__ = []
