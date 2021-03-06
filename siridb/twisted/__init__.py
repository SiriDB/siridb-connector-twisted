import os
import sys
from .lib.client import SiriDBClientTwisted


__version_info__ = (2, 0, 6)
__version__ = '.'.join(map(str, __version_info__))
__maintainer__ = 'Jeroen van der Heijden'
__email__ = 'jeroen@transceptor.technology'
__all__ = [
    'SiriDBClientTwisted'
]
