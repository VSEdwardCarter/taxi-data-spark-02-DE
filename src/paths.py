from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_NYC_DIR = PROJECT_ROOT / "data" / "raw" / "kaggle" / "nyc_taxi"
BRONZE_DIR  = PROJECT_ROOT / "data" / "processed" / "bronze" / "nyc_taxi"
SILVER_DIR  = PROJECT_ROOT / "data" / "processed" / "silver" / "nyc_taxi"
GOLD_DIR    = PROJECT_ROOT / "data" / "processed" / "gold" / "nyc_taxi"

def s(p: Path) -> str:
    return str(p)