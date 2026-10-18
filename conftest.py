# conftest.py - makes the project root the sys.path anchor for pytest
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
