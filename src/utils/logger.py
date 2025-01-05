import logging
import os
from datetime import datetime

def setup_logger():
    """設置日誌系統"""
    try:
        # 創建logs目錄
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # 設置日誌文件名
        log_file = os.path.join(
            log_dir, 
            f'racing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        
        # 測試文件是否可寫
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write("Initializing log file...\n")
        
        # 配置日誌格式
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info("Logger initialized successfully")
        return logger
        
    except Exception as e:
        print(f"Error setting up logger: {e}")
        # 返回一個基本的控制台logger作為後備
        return logging.getLogger(__name__) 