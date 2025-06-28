#!/usr/bin/env python3
"""
Run database normalization with progress tracking
"""

import os
import sys
import signal
from normalize_database_names import NameNormalizer

# Handle graceful shutdown
def signal_handler(sig, frame):
    print('\nNormalization interrupted by user')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def main():
    print("Starting database normalization...")
    print("This process may take several minutes. Press Ctrl+C to stop.")
    
    normalizer = NameNormalizer()
    try:
        normalizer.normalize_database()
        print("\nNormalization completed successfully!")
    except Exception as e:
        print(f"\nError during normalization: {e}")
        sys.exit(1)
    finally:
        normalizer.close()

if __name__ == "__main__":
    main()