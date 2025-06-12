#!/usr/bin/env python3
"""Entry point script for running the database browser GUI."""

import os
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now we can import from src package
from src.gui.db_browser import main

if __name__ == "__main__":
    main() 