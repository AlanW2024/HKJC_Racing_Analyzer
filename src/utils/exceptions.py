class RaceScraperError(Exception):
    """爬蟲基礎異常"""
    pass

class NetworkError(RaceScraperError):
    """網絡相關異常"""
    pass

class DataProcessError(RaceScraperError):
    """數據處理異常"""
    pass

class ConfigError(RaceScraperError):
    """配置相關異常"""
    pass 