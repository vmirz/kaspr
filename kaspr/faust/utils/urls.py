"""URL utilities - Working with URLs."""
import re
from typing import List, Optional, Union
from yarl import URL


def urllist(arg: Union[URL, str, List[str], List[URL]], *,
            default_scheme: str = None) -> List[URL]:
    """Create list of URLs.

    You can pass in a string of URLs separated by commas or semicolons, or an
    actual list, and this will convert that into a list of
    :class:`yarl.URL` objects.

    URLs without a scheme take that of the first, or else the default.
    """
    if isinstance(arg, URL):
        arg = [arg]
    elif isinstance(arg, str):
        arg = [u.strip() for u in re.split('[,;]', arg) if u.strip()]
    if not arg:
        raise ValueError('URL argument cannot be falsy')

    scheme = _scheme(arg[0]) or default_scheme
    return [_ensure_scheme(scheme, u) for u in arg]


def _scheme(url: Union[str, URL]) -> Optional[str]:
    # URL('localhost:9092').scheme is 'localhost'.
    if isinstance(url, str) and '://' not in url:
        return None
    return URL(url).scheme


def _ensure_scheme(default_scheme: Optional[str],
                   url: Union[str, URL]) -> URL:
    """Ensure URL has a default scheme.

    An URL like "localhost" will be returned with the default scheme
    added, while an URL with existing scheme is returned unmodified.
    """
    if default_scheme and not _scheme(url):
        return URL(f'{default_scheme}://{str(url).removeprefix("//")}')
    return URL(url)
