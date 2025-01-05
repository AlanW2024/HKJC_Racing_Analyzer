import asyncio
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def async_retry(max_retries=3, delay=1):
    """異步重試裝飾器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"嘗試 {attempt + 1}/{max_retries} 失敗: {str(e)}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay * (attempt + 1))
                        
            logger.error(f"重試{max_retries}次後仍然失敗")
            raise last_exception
            
        return wrapper
    return decorator 