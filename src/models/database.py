import logging
from typing import List, Dict
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, select, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from datetime import datetime

logger = logging.getLogger(__name__)

Base = declarative_base()

class RaceResult(Base):
    __tablename__ = 'race_results'
    
    id = Column(Integer, primary_key=True)
    race_id = Column(String(50))
    race_date = Column(String(10))  # YYYY-MM-DD
    race_number = Column(Integer)
    horse_no = Column(String(10))
    horse_name = Column(String(100))
    draw = Column(Integer)
    finish_position = Column(Integer)
    jockey = Column(String(50))
    trainer = Column(String(50))
    finish_time = Column(String(20))
    odds = Column(Float)
    distance = Column(Integer)
    race_info = Column(Text)

    def __repr__(self):
        return f"<RaceResult(race_id={self.race_id}, horse_name={self.horse_name})>" 

class JockeyStats(Base):
    __tablename__ = 'jockey_stats'
    
    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    jockey = Column(String(50))
    total_races = Column(Integer)
    wins = Column(Integer)
    win_rate = Column(Float)
    avg_position = Column(Float) 

class DataStorage:
    def __init__(self, db_config: dict):
        """初始化数据存储"""
        connection_string = (
            f"mysql+aiomysql://{db_config['USER']}:{db_config['PASSWORD']}"
            f"@{db_config['HOST']}:{db_config['PORT']}/{db_config['NAME']}"
        )
        self.engine = create_async_engine(connection_string)
        self.session = sessionmaker(self.engine)  # 使用同步会话

    async def initialize(self):
        """初始化数据库表"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def get_race_results(self, start_date: str, end_date: str) -> List[RaceResult]:
        """获取日期范围内的赛事"""
        try:
            with self.session() as session:
                query = select(RaceResult).where(
                    and_(
                        RaceResult.race_date >= start_date,
                        RaceResult.race_date <= end_date
                    )
                )
                result = session.execute(query)
                return result.scalars().all()
        except Exception as e:
            logger.error(f"查询日期范围 {start_date} 至 {end_date} 的数据时出错: {e}")
            return []

    get_races_by_date_range = get_race_results

    def close(self):  # 改为同步方法
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()

    async def save_races(self, races: List[Dict]) -> bool:
        """保存赛事数据"""
        try:
            async with self.async_session() as session:
                for race_data in races:
                    race = RaceResult(**race_data)
                    session.add(race)
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"保存赛事数据时出错: {e}")
            return False

    async def save_analysis_results(self, results: List[Dict]) -> bool:
        """保存分析结果"""
        try:
            async with self.async_session() as session:
                for result in results:
                    stats = JockeyStats(**result)
                    session.add(stats)
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"保存分析结果时出错: {e}")
            return False 