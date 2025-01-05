import aiomysql
from contextlib import asynccontextmanager
from typing import List, Dict, Any
import pickle

class DatabaseConnection:
    def __init__(self, config: dict):
        self.config = config
        self.pool = None

    async def init(self):
        """初始化连接池"""
        self.pool = await aiomysql.create_pool(
            host=self.config['host'],
            user=self.config['user'],
            password=self.config['password'],
            db=self.config['database'],
            autocommit=True,
            charset='utf8mb4'
        )
        # 初始化后立即创建表
        await self._create_tables()

    async def _create_tables(self):
        """创建必要的数据表"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS race_cache (
                        date VARCHAR(20) PRIMARY KEY,
                        data LONGBLOB,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                await conn.commit()

    @asynccontextmanager
    async def get_conn(self):
        """获取数据库连接"""
        async with self.pool.acquire() as conn:
            yield conn

    async def close(self):
        """关闭连接池"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed() 

    async def store_race_data(self, date: str, data: List[Dict[str, Any]]):
        """存储赛事数据"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 先删除已存在的数据
                await cursor.execute(
                    "DELETE FROM race_cache WHERE date = %s",
                    (date,)
                )
                # 再插入新数据
                await cursor.execute(
                    "INSERT INTO race_cache (date, data, updated_at) VALUES (%s, %s, NOW())",
                    (date, pickle.dumps(data))
                )
                await conn.commit() 