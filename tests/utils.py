"""
Compatibility shim for old utils.py imports.

This file maintains backward compatibility while the utils have been
reorganized into focused modules. All imports are re-exported from the
new structure.

New code should import directly from tests.utils submodules for better
organization and IDE support.
"""

# Re-export everything from the new structure for backward compatibility
from tests.utils import *  # noqa: F401, F403
