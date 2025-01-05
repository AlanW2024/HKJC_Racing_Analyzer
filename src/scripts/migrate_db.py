import os
import sys
import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import mysql.connector
import yaml

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    with open('config/settings.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def create_mysql_database():
    """创建 MySQL 数据库"""
    config = load_config()['DATABASE']
    try:
        conn = mysql.connector.connect(
            host=config['HOST'],
            user=config['USER'],
            password=config['PASSWORD']
        )
        cursor = conn.cursor()
        
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['NAME']}")
        logger.info(f"数据库 {config['NAME']} 创建成功")
        
    except Exception as e:
        logger.error(f"创建数据库失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def migrate_database():
    """数据库迁移"""
    try:
        # 加载配置
        config = load_config()['DATABASE']
        
        # 创建数据库
        create_mysql_database()
        
        # 创建数据库连接
        db_url = (f"mysql+mysqlconnector://{config['USER']}:{config['PASSWORD']}@"
                 f"{config['HOST']}:{config['PORT']}/{config['NAME']}")
        
        engine = create_engine(db_url, echo=True)
        
        # 导入数据库模型
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
        from src.models.database import Base, RaceResult
        
        # 删除旧表
        logger.info("正在删除旧表...")
        Base.metadata.drop_all(engine)
        
        # 创建新表
        logger.info("正在创建新表结构...")
        Base.metadata.create_all(engine)
        
        logger.info("数据库迁移完成！")
        
    except Exception as e:
        logger.error(f"数据库迁移失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_database() 