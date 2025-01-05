from typing import Optional
import asyncio
import logging
import signal

logger = logging.getLogger(__name__)

class ResourceManager:
    def __init__(self):
        self.scraper = None
        self.storage = None
        
    async def cleanup(self):
        """清理资源"""
        try:
            if self.scraper:
                logger.info('正在关闭爬虫...')
                await self.scraper.close()
            
            if self.storage:
                logger.info('正在关闭数据库连接...')
                self.storage.close()
                
        except Exception as e:
            logger.error(f"清理资源时出错: {e}")
    
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup() 