import logging
logger = logging.getLogger(__name__)

from datetime import datetime
import re
from typing import Dict, Any, List

class RaceDataPreprocessor:
    """賽馬數據預處理器"""
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any]) -> bool:
        """驗證必要欄位"""
        required_fields = [
            'race_id', 'horse_no', 'finish_position',
            'jockey', 'odds', 'race_date', 'distance', 'draw'
        ]
        return all(field in data for field in required_fields)
    
    @staticmethod
    def clean_horse_name(name: str) -> tuple:
        """處理馬匹名稱，分離編號"""
        pattern = r"(.*?)\s*\((\w+)\)"
        match = re.match(pattern, name)
        if match:
            return match.group(1).strip(), match.group(2)
        return name, ""
    
    @staticmethod
    def normalize_date(date_str: str) -> str:
        """標準化日期格式"""
        try:
            date_obj = datetime.strptime(date_str, "%Y/%m/%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return date_str
            
    def process_race_data(self, raw_data: List[Dict]) -> List[Dict]:
        """處理原始賽事數據"""
        processed_data = []
        
        for race in raw_data:
            try:
                if not self.validate_required_fields(race):
                    continue
                    
                # 清理和轉換數據
                race['race_date'] = self.normalize_date(race['race_date'])
                horse_name, horse_code = self.clean_horse_name(race['horse_name'])
                race['horse_name'] = horse_name
                race['horse_code'] = horse_code
                
                # 轉換數值型欄位
                race['finish_position'] = int(race['finish_position']) if race['finish_position'] != 'WV' else 99
                race['odds'] = float(race['odds']) if race['odds'] != '---' else 999.0
                race['distance'] = int(str(race['distance']).replace('m', ''))
                race['draw'] = int(race['draw'])
                
                processed_data.append(race)
                
            except Exception as e:
                logger.error(f"處理數據時出錯: {e}, 數據: {race}")
                continue
                
        return processed_data 