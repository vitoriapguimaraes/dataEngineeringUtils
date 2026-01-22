from pathlib import Path

# Get the path to the 'utils' directory
UTILS_DIR = Path(__file__).parent.absolute()

# Get the project root directory (parent of 'utils')
PROJECT_ROOT = UTILS_DIR.parent

# Define the data directory
DATA_DIR = PROJECT_ROOT / "data"
