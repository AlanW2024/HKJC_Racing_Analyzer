import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.database import DB_CONFIG

def init_mysql():
    # 先连接到MySQL服务器（不指定数据库）
    conn = mysql.connector.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    cursor = conn.cursor()

    try:
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"数据库 {DB_CONFIG['database']} 创建成功")

        # 切换到新创建的数据库
        cursor.execute(f"USE {DB_CONFIG['database']}")

        # 创建表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS race_cache (
                date VARCHAR(20) PRIMARY KEY,
                data LONGBLOB,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("表创建成功")

    except Exception as e:
        print(f"初始化失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    init_mysql() 