import asyncio
from typing import List, Dict
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
from src.services.scraper import RaceScraper
from src.services.storage import DataStorage

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, scraper: RaceScraper, storage: DataStorage, config: Dict):
        self.scraper = scraper
        self.storage = storage
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.max_concurrent = config.get('MAX_CONCURRENT', 5)  # 最大并发数
        
    async def process_date_range(self, start_date: str, end_date: str):
        """并发处理日期范围内的数据"""
        dates = self._generate_dates(start_date, end_date)
        total_dates = len(dates)
        
        # 创建进度条
        pbar = tqdm(
            total=total_dates,
            desc="处理进度",
            unit="天",
            ncols=100
        )
        
        # 创建信号量控制并发
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # 创建所有任务
        tasks = []
        for date in dates:
            task = self._process_single_date(date, semaphore, pbar)
            tasks.append(task)
        
        # 并发执行任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        total_records = 0
        success_dates = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"处理出错: {result}")
            elif result:
                total_records += result
                success_dates += 1
        
        # 关闭进度条
        pbar.close()
        
        # 显示统计信息
        logger.info(f"\n批次处理完成:")
        logger.info(f"- 总天数: {total_dates}")
        logger.info(f"- 成功天数: {success_dates}")
        logger.info(f"- 获取记录: {total_records}")
        logger.info(f"- 成功率: {(success_dates/total_dates*100):.1f}%")
        
    async def _process_single_date(self, date: str, semaphore: asyncio.Semaphore, pbar: tqdm) -> int:
        """处理单个日期的数据"""
        try:
            async with semaphore:
                result = await self._fetch_and_save_data(date)
                pbar.update(1)  # 更新进度条
                if result > 0:
                    pbar.set_postfix({"最新": date, "记录": result})
                return result
        except Exception as e:
            logger.error(f"处理 {date} 时出错: {e}")
            pbar.update(1)  # 即使出错也更新进度
            return 0
            
    async def _fetch_and_save_data(self, date: str) -> int:
        """获取并保存数据"""
        try:
            # 检查是否已有数据
            existing = self.storage.get_race_results(date, date)
            if existing:
                return 0
            
            # 获取数据
            race_data = await self.scraper.scrape_race_data(date)
            if not race_data:
                return 0
            
            # 保存数据
            self.storage.save_race_results(race_data)
            return len(race_data)
            
        except Exception as e:
            logger.error(f"获取/保存数据时出错 ({date}): {e}")
            return 0
    
    @staticmethod
    def _generate_dates(start_date: str, end_date: str) -> List[str]:
        """生成日期范围内的所有日期"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = []
        
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
            
        return dates 

    async def process_date(self, date: datetime) -> bool:
        """处理单个日期的数据"""
        date_str = date.strftime("%Y-%m-%d")
        
        # 首先检查数据库中是否已有该日期的数据
        existing_data = await self.storage.get_races_by_date(date_str)
        if existing_data:
            self.logger.info(f"日期 {date_str} 的数据已存在于数据库中，跳过抓取")
            return True
            
        # 如果数据库中没有，才进行抓取
        try:
            races = await self.scraper.get_races(date)
            if races:
                await self.storage.save_races(races)
                self.logger.info(f"成功爬取 {date_str} 的赛事数据，共 {len(races)} 条记录")
                return True
            else:
                self.logger.warning(f"日期 {date_str} 没有找到赛事数据")
                return False
        except Exception as e:
            self.logger.error(f"处理日期 {date_str} 时出错: {e}")
            return False 