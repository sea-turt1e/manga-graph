"""Environment bootstrap module.

Importing this module will load environment variables from a .env file
so that lower layers don't need to call load_dotenv themselves.
"""

from dotenv import load_dotenv as _load_dotenv

# Load environment variables from .env if present
_load_dotenv()
