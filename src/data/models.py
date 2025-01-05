from dataclasses import dataclass
from typing import Dict

@dataclass
class RaceData:
    """赛事数据结构"""
    date: str
    race_no: int
    horse_name: str
    jockey: str
    trainer: str
    finish_time: str
    odds: float
    rank: int
    race_info: Dict[str, str] 