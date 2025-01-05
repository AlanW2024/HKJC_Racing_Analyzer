from src.data.cache import DataCache
from src.scrapers.race_scraper import RaceScraper
from src.config.config import Config

class RaceDataAgent:
    def __init__(
        self,
        cache: DataCache,
        scraper: RaceScraper,
        config: Config
    ):
        self.cache = cache
        self.scraper = scraper
        self.config = config 