"""Load-time defaults for the shish library."""

import locale

DEFAULT_ENCODING: str = locale.getpreferredencoding(False)
