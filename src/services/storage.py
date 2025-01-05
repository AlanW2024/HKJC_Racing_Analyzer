from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any
import pandas as pd
from src.models.database import Base, RaceResult, JockeyStats
import logging
from sqlalchemy.dialects.mysql import insert

logger = logging.getLogger(__name__)

class DataStorage:
    def __init__(self, config):
        """初始化數據庫連接"""
        self.connection_string = (
            f"mysql+{config['DRIVER']}://{config['USER']}:{config['PASSWORD']}@"
            f"{config['HOST']}:{config['PORT']}/{config['DATABASE']}")
        
        self.engine = create_engine(
            self.connection_string,
            pool_size=config['POOL_SIZE'],
            max_overflow=config['MAX_OVERFLOW'],
            pool_timeout=config['POOL_TIMEOUT'],
            echo=config['ECHO']
        )
        
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
    def save_race_results(self, results: List[Dict]):
        """批量保存赛事结果"""
        if not results:
            return
            
        session = self.Session()
        try:
            # 准备批量插入数据
            values = []
            for result in results:
                race_id = ''.join(filter(str.isdigit, str(result.get('race_id', '0'))))
                values.append({
                    'race_id': race_id,
                    'race_date': result.get('race_date'),
                    'race_number': int(race_id or '0'),
                    'horse_no': result.get('horse_no'),
                    'horse_name': result.get('horse_name'),
                    'draw': result.get('draw'),
                    'finish_position': result.get('finish_position', 99),
                    'jockey': result.get('jockey'),
                    'trainer': result.get('trainer'),
                    'finish_time': result.get('finish_time'),
                    'odds': result.get('odds', 0.0),
                    'distance': result.get('distance', 0),
                    'race_info': result.get('race_info')
                })
            
            # 使用 UPSERT 语句
            stmt = insert(RaceResult).values(values)
            stmt = stmt.on_duplicate_key_update({
                'finish_position': stmt.inserted.finish_position,
                'odds': stmt.inserted.odds,
                'finish_time': stmt.inserted.finish_time
            })
            
            session.execute(stmt)
            session.commit()
            logger.info(f"成功保存 {len(results)} 条赛事记录")
            
        except Exception as e:
            session.rollback()
            logger.error(f"保存赛事结果时出错: {e}")
            raise
        finally:
            session.close()
            
    def get_jockey_stats(self, start_date=None, end_date=None):
        """獲取騎師統計"""
        query = """
        SELECT 
            jockey,
            COUNT(*) as total_races,
            SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) as wins,
            AVG(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) * 100 as win_rate,
            AVG(finish_position) as avg_position
        FROM race_results
        WHERE race_date BETWEEN :start_date AND :end_date
        GROUP BY jockey
        """
        return pd.read_sql(query, self.engine, params={
            'start_date': start_date,
            'end_date': end_date
        }) 

    def get_race_results(self, start_date, end_date):
        """获取指定日期范围内的赛马结果"""
        session = self.Session()
        try:
            # 先检查数据库中是否有数据
            total_count = session.query(RaceResult).count()
            logger.info(f"数据库中总共有 {total_count} 条记录")
            
            # 检查日期格式
            logger.info(f"查询日期格式: start_date={start_date}, end_date={end_date}")
            
            # 查看一些样本数据的日期格式
            sample = session.query(RaceResult.race_date).limit(3).all()
            logger.info(f"数据库中的日期格式样本: {[s.race_date for s in sample]}")
            
            # 查询指定日期范围的数据
            results = session.query(RaceResult).filter(
                RaceResult.race_date.between(start_date, end_date)
            ).order_by(RaceResult.race_date).all()
            
            logger.info(f"查询到 {len(results)} 条记录")
            
            if results:
                # 显示前几条记录的详细信息
                for i, result in enumerate(results[:3]):
                    logger.info(f"示例记录 {i+1}: "
                              f"日期={result.race_date}, "
                              f"骑师={result.jockey}, "
                              f"名次={result.finish_position}")
            else:
                # 尝试直接查询所有记录的日期
                all_dates = session.query(RaceResult.race_date).distinct().all()
                logger.warning(f"未找到任何记录，数据库中存在的日期: {[d.race_date for d in all_dates]}")
                
            return results
        except Exception as e:
            logger.error(f"获取赛马结果时出错: {e}")
            logger.exception(e)
            return []
        finally:
            session.close()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'engine'):
            self.engine.dispose() 

    def save_analysis_results(self, results: List[Dict[str, Any]]):
        """保存分析结果"""
        session = self.Session()
        try:
            for result in results:
                # 检查是否已存在相同记录
                existing = session.query(JockeyStats).filter_by(
                    date=result['date'],
                    jockey=result['jockey']
                ).first()
                
                if existing:
                    # 更新现有记录
                    for key, value in result.items():
                        setattr(existing, key, value)
                else:
                    # 创建新记录
                    stats = JockeyStats(**result)
                    session.add(stats)
            
            session.commit()
            logger.info(f"成功保存 {len(results)} 条分析结果")
        except Exception as e:
            session.rollback()
            logger.error(f"保存分析结果时出错: {e}")
        finally:
            session.close() 