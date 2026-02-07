from pathlib import Path

PROJECT_ROOT =  Path(__file__).resolve().parents[1]


RAW_NYC_DIR = PROJECT_ROOT / "data" / "raw" / "kaggle" / "nyc_taxi"
BRONZE_DIR = PROJECT_ROOT / "data" / "baseline" / "processed" / "bronze"
SILVER_DIR = PROJECT_ROOT  / "data" / "baseline" / "processed" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "baseline" / "processed" / "gold"
NOVICE_BRONZE_DIR = PROJECT_ROOT / "data" / "novice" / "processed" / "bronze"
NOVICE_SILVER_DIR = PROJECT_ROOT  / "data" / "novice" / "processed" / "silver"
NOVICE_GOLD_DIR = PROJECT_ROOT / "data" / "novice" / "processed" / "gold"
SILVER_INVEST_DIR = SILVER_DIR / "investigation_trips"
SILVER_CLEAN_DIR  = SILVER_DIR / "trips_clean"


def s(p: Path) -> str:
    return str(p)
