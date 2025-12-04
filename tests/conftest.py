import sys
import os

# This code adds the project's root directory (the one containing
# aggregator_server.py) to Python's path. This allows pytest to find
# and import your application modules like 'aggregator_server' and 'aggregator_utils'.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
