from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass(frozen=True)
class DQRule:
    name: str
    condition: str