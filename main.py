import asyncio
import logging
from datetime import datetime, timedelta
import signal
import sys
from src.services.scraper import RaceScraper
from src.services.analyzer import RaceAnalyzer
from src.services.storage import DataStorage
from src.services.batch_processor import BatchProcessor
from src.services.visualizer import RaceVisualizer
from src.utils.logger import setup_logger
from src.utils.formatter import format_analysis_results
import os
import yaml
from src.services.resource_manager import ResourceManager
from typing import Dict, Any
import mysql.connector

# 設置日誌
logger = setup_logger()

# 讀取設定檔 - 使用 utf-8 編碼
try:
    with open('config/settings.yaml', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    logger.error("找不到 settings.yaml 文件")
    sys.exit(1)
except Exception as e:
    logger.error(f"讀取配置文件時發生錯誤: {e}")
    sys.exit(1)

# 建立資料庫連接
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="racing_db",
    port=3306
)

# 測試連接
try:
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    print(f"Database version: {version[0]}")
    logger.info(f"Database version: {version[0]}")
except Exception as e:
    logger.error(f"資料庫連接失敗: {e}")
    sys.exit(1)

def load_config():
    """加载配置文件"""
    with open('config/settings.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def display_yearly_stats(stats: Dict[str, Any], start_date: str, end_date: str):
    """显示年度统计数据"""
    logger.info("\n" + "="*70)
    logger.info(f"年度统计数据 ({start_date} 至 {end_date})")
    logger.info("="*70)
    
    # 显示总体统计
    summary = stats['summary']
    logger.info("\n总体统计:")
    logger.info(f"- 总赛事数: {summary['total_races']:,} 场")
    logger.info(f"- 赛事天数: {summary['total_race_days']:,} 天")
    logger.info(f"- 现役骑师: {summary['active_jockeys']:,} 人")
    logger.info(f"- 日均赛事: {summary['avg_races_per_day']:.1f} 场")
    
    # 显示胜率最高的骑师
    logger.info("\n胜率最高骑师 (前5名):")
    logger.info(f"{'骑师':<8} | {'总赛事':>6} | {'总胜场':>6} | {'胜率':>6} | {'平均名次':>6}")
    logger.info("-" * 45)
    for jockey in stats['top_jockeys']:
        logger.info(
            f"{jockey['jockey']:<8} | "
            f"{jockey['total_races']:>6} | "
            f"{jockey['total_wins']:>6} | "
            f"{jockey['win_rate']:>5.1f}% | "
            f"{jockey['avg_position']:>6.2f}"
        )
    
    # 添加赔率分析显示
    if 'odds_analysis' in stats:
        odds = stats['odds_analysis']
        logger.info("\n赔率分析:")
        logger.info(f"- 平均赔率: {odds['overall']['avg_odds']:>6.2f}")
        logger.info(f"- 获胜平均赔率: {odds['winners']['avg_winning_odds']:>6.2f}")
        logger.info(f"- 最高获胜赔率: {odds['winners']['highest_odds_winner']:>6.2f}")
        
        # 计算最长骑师名称长度
        max_name_length = max(len(j['jockey']) for j in odds['jockey_odds'])
        name_width = max(max_name_length + 2, 10)  # 确保至少10个字符宽
        
        # 显示骑师赔率分析
        logger.info("\n骑师赔率分析 (按胜率排序):")
        logger.info(f"{'骑师':<{name_width}} | {'平均赔率':>8} | {'最低赔率':>8} | {'最高赔率':>8} | {'胜率':>6}")
        logger.info("-" * (name_width + 47))
        
        for jockey in sorted(odds['jockey_odds'], key=lambda x: x['win_rate'], reverse=True):
            if jockey['mean'] > 0:  # 只显示有效赔率的骑师
                logger.info(
                    f"{jockey['jockey']:<{name_width}} | "
                    f"{jockey['mean']:>8.2f} | "
                    f"{jockey['min']:>8.2f} | "
                    f"{jockey['max']:>8.2f} | "
                    f"{jockey['win_rate']:>5.1f}%"
                )
        
        # 显示高赔爆冷获胜
        logger.info("\n重大爆冷获胜 (前5名):")
        logger.info(f"{'骑师':<{name_width}} | {'赔率':>8} | {'日期':>10}")
        logger.info("-" * (name_width + 27))
        for race in odds['upset_wins']:
            logger.info(
                f"{race['jockey']:<{name_width}} | "
                f"{race['odds']:>8.1f} | "
                f"{race['race_date']:>10}"
            )
    
    logger.info("\n" + "="*70 + "\n")

async def main():
    async with ResourceManager() as rm:
        config = load_config()
        logger.info("初始化系统组件...")
        
        try:
            # 初始化存储和分析器
            rm.storage = DataStorage(config['DATABASE'])
            analyzer = RaceAnalyzer()
            
            # 设置固定的日期范围
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2025, 1, 1)
            
            # 直接检查数据库中的数据
            existing_data = rm.storage.get_race_results(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            if existing_data:
                logger.info(f"数据库中已有 {len(existing_data)} 条记录，直接进行分析")
                # ... 继续分析流程
            else:
                # 只有在没有数据时才初始化爬虫
                rm.scraper = RaceScraper(config['SCRAPER'])
                await rm.scraper.init()
                batch_processor = BatchProcessor(rm.scraper, rm.storage, config)
                
                # 按季度分批处理
                current_date = start_date
                while current_date < end_date:
                    try:
                        # 计算当前批次的结束日期（3个月）
                        batch_end = min(
                            current_date + timedelta(days=90),  # 一个季度
                            end_date
                        )
                        
                        batch_start_str = current_date.strftime("%Y-%m-%d")
                        batch_end_str = batch_end.strftime("%Y-%m-%d")
                        
                        logger.info(f"\n处理季度数据: {batch_start_str} 至 {batch_end_str}")
                        
                        # 使用并发处理
                        await batch_processor.process_date_range(
                            batch_start_str,
                            batch_end_str
                        )
                        
                        # 分析当前季度数据
                        existing_data = rm.storage.get_race_results(
                            batch_start_str,
                            batch_end_str
                        )
                        
                        if existing_data:
                            analysis_results = analyzer.analyze_races(existing_data)
                            if analysis_results:
                                rm.storage.save_analysis_results(analysis_results)
                                display_analysis_results(
                                    analysis_results, 
                                    batch_start_str, 
                                    batch_end_str
                                )
                    
                    except Exception as e:
                        logger.error(f"处理季度出错: {e}")
                        continue
                    
                    finally:
                        # 移动到下一个季度
                        current_date = batch_end + timedelta(days=1)
                        await asyncio.sleep(1)
            
            # 获取并分析年度数据
            yearly_data = rm.storage.get_race_results(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            if yearly_data:
                yearly_stats = analyzer.analyze_yearly_stats(
                    [result.__dict__ for result in yearly_data]
                )
                display_yearly_stats(
                    yearly_stats,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d")
                )
            
            logger.info("\n=== 年度数据处理完成 ===")
            
        except Exception as e:
            logger.error(f"处理过程出错: {e}")
            raise

def display_analysis_results(results, start_date, end_date):
    """显示分析结果"""
    logger.info("\n" + "="*50)
    logger.info(f"骑师统计数据 ({start_date} 至 {end_date})")
    logger.info("="*50)
    
    # 按胜率排序
    sorted_results = sorted(
        results, 
        key=lambda x: (x['win_rate'], -x['avg_position']), 
        reverse=True
    )
    
    # 计算最长骑师名称
    max_name_length = max(len(r['jockey']) for r in sorted_results)
    
    # 输出表头
    logger.info(f"\n{'骑师':<{max_name_length}} | {'总赛事':>4} | {'获胜':>4} | {'胜率':>6} | {'平均名次':>6}")
    logger.info("-" * (max_name_length + 32))
    
    # 输出统计数据
    for result in sorted_results:
        logger.info(
            f"{result['jockey']:<{max_name_length}} | "
            f"{result['total_races']:>4d} | "
            f"{result['wins']:>4d} | "
            f"{result['win_rate']:>5.1f}% | "
            f"{result['avg_position']:>6.2f}"
        )
    
    logger.info("\n" + "="*50)
    logger.info(f"分析完成，共处理 {len(results)} 位骑师的数据")
    logger.info("="*50 + "\n")

def display_jockey_odds_analysis(jockey_odds):
    """显示骑师赔率分析"""
    logger.info("\n骑师赔率分析 (按胜率排序):")
    logger.info(f"{'骑师':<8} | {'平均赔率':>8} | {'最低赔率':>8} | {'最高赔率':>8} | {'胜率':>6}")
    logger.info("-" * 55)
    
    for jockey in sorted(jockey_odds, key=lambda x: x['win_rate'], reverse=True):
        if jockey['mean'] > 0:  # 只显示有效赔率的骑师
            logger.info(
                f"{jockey['jockey']:<10} | "
                f"{jockey['mean']:>8.2f} | "
                f"{jockey['min']:>8.2f} | "
                f"{jockey['max']:>8.2f} | "
                f"{jockey['win_rate']:>5.1f}%"
            )

if __name__ == "__main__":
    rm = ResourceManager()  # 创建资源管理器实例
    try:
        # 禁用 ResourceWarning
        import warnings
        warnings.filterwarnings("ignore", category=ResourceWarning)
        
        # Windows 上使用 ProactorEventLoop
        if sys.platform.startswith('win'):
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
        else:
            loop = asyncio.get_event_loop()
        
        # 測試日誌系統
        logger = setup_logger()
        logger.info("=== Starting Program ===")
        logger.info("Testing logging system...")
        
        # 檢查日誌文件
        log_dir = 'logs'
        if os.path.exists(log_dir):
            files = os.listdir(log_dir)
            logger.info(f"Log files found: {files}")
        else:
            logger.warning("Logs directory not found!")
            
        # 設置信號處理
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda s, _: asyncio.create_task(rm.cleanup(s)))
        
        # 運行主程序
        loop.run_until_complete(main())
        
    finally:
        try:
            # 确保事件循环正确关闭
            if loop and loop.is_running():
                loop.run_until_complete(rm.cleanup())  # 使用 rm.cleanup
                loop.run_until_complete(asyncio.sleep(0.1))
            
            # 关闭所有待处理的任务
            if loop and not loop.is_closed():
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                
                if pending:
                    loop.run_until_complete(asyncio.wait(pending, timeout=5))
                
                # 关闭事件循环
                loop.close()
            
        except Exception as e:
            logger.error(f"關閉事件循環時出錯: {e}")
        finally:
            # 清理全局引用
            loop = None
            scraper = None
            storage = None
            # 确保日志正确关闭
            logging.shutdown() 