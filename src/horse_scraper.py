import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import yaml
import logging

class HorseRacingScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/racing/LocalResults.aspx"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='horse_scraping.log'
        )

    def get_race_results(self, race_date):
        """获取指定日期的赛事结果"""
        try:
            params = {
                'date': race_date.strftime('%Y/%m/%d')
            }
            response = requests.get(self.base_url, params=params, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
        except Exception as e:
            logging.error(f"获取赛事结果失败: {str(e)}")
            return None

    def parse_horse_info(self, horse_row):
        """解析马匹基本信息"""
        try:
            return {
                'horse_no': horse_row.find('td', {'class': 'horse_no'}).text.strip(),
                'horse_name': horse_row.find('td', {'class': 'horse_name'}).text.strip(),
                'jockey': horse_row.find('td', {'class': 'jockey'}).text.strip(),
                'trainer': horse_row.find('td', {'class': 'trainer'}).text.strip(),
                'actual_weight': horse_row.find('td', {'class': 'weight'}).text.strip(),
                'draw': horse_row.find('td', {'class': 'draw'}).text.strip(),
                'running_position': horse_row.find('td', {'class': 'running_position'}).text.strip(),
                'finish_time': horse_row.find('td', {'class': 'finish_time'}).text.strip(),
                'win_odds': horse_row.find('td', {'class': 'win_odds'}).text.strip()
            }
        except Exception as e:
            logging.error(f"解析马匹信息失败: {str(e)}")
            return None

    def get_horse_history(self, horse_code):
        """获取马匹历史成绩"""
        try:
            url = f"https://racing.hkjc.com/racing/information/Chinese/Horse/HorseResults.aspx?Horse={horse_code}"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            history_data = []
            history_table = soup.find('table', {'class': 'performance_table'})
            
            if history_table:
                for row in history_table.find_all('tr')[1:]:  # 跳过表头
                    cols = row.find_all('td')
                    if len(cols) >= 10:
                        history_data.append({
                            'race_date': cols[0].text.strip(),
                            'track': cols[1].text.strip(),
                            'distance': cols[2].text.strip(),
                            'position': cols[3].text.strip(),
                            'rating': cols[4].text.strip()
                        })
            return history_data
        except Exception as e:
            logging.error(f"获取马匹历史记录失败: {str(e)}")
            return []

    def save_to_yaml(self, data, filename):
        """保存数据到YAML文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
            logging.info(f"数据已保存到 {filename}")
        except Exception as e:
            logging.error(f"保存YAML文件失败: {str(e)}")

    def get_horse_profile(self, horse_id):
        """获取马匹详细资料和往绩"""
        try:
            url = f"https://racing.hkjc.com/racing/information/Chinese/Horse/Horse.aspx?HorseId={horse_id}"
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取基本信息
            basic_info = {}
            info_table = soup.find_all('table')[0]  # 第一个表格包含基本信息
            
            # 解析基本信息
            basic_info = {
                'origin_age': info_table.find(text=lambda t: '出生地 / 馬齡' in str(t)).find_next(text=True).strip(),
                'color_sex': info_table.find(text=lambda t: '毛色 / 性別' in str(t)).find_next(text=True).strip(),
                'import_type': info_table.find(text=lambda t: '進口類別' in str(t)).find_next(text=True).strip(),
                'season_stakes': info_table.find(text=lambda t: '今季獎金' in str(t)).find_next(text=True).strip(),
                'total_stakes': info_table.find(text=lambda t: '總獎金' in str(t)).find_next(text=True).strip(),
                'record': info_table.find(text=lambda t: '冠-亞-季-總出賽次數' in str(t)).find_next(text=True).strip(),
                'trainer': info_table.find('a', href=lambda h: 'Trainers' in str(h)).text.strip(),
                'owner': info_table.find('a', href=lambda h: 'OwnerSearch' in str(h)).text.strip(),
                'current_rating': info_table.find(text=lambda t: '現時評分' in str(t)).find_next(text=True).strip(),
                'season_start_rating': info_table.find(text=lambda t: '季初評分' in str(t)).find_next(text=True).strip(),
                'sire': info_table.find(text=lambda t: '父系' in str(t)).find_next('a').text.strip(),
                'dam': info_table.find(text=lambda t: '母系' in str(t)).find_next(text=True).strip(),
                'dam_sire': info_table.find(text=lambda t: '外祖父' in str(t)).find_next(text=True).strip()
            }

            # 获取往绩记录
            race_history = []
            history_table = soup.find('table', {'class': 'performance'})
            if history_table:
                for row in history_table.find_all('tr')[1:]:  # 跳过表头
                    cols = row.find_all('td')
                    if len(cols) >= 15:  # 确保行有足够的列
                        race_record = {
                            'season': cols[0].text.strip(),
                            'race_no': cols[1].text.strip(),
                            'date': cols[2].text.strip(),
                            'track': cols[3].text.strip(),
                            'distance': cols[4].text.strip(),
                            'track_condition': cols[5].text.strip(),
                            'class': cols[6].text.strip(),
                            'draw': cols[7].text.strip(),
                            'rating': cols[8].text.strip(),
                            'trainer': cols[9].text.strip(),
                            'jockey': cols[10].text.strip(),
                            'finish_position': cols[11].text.strip(),
                            'win_odds': cols[12].text.strip(),
                            'actual_weight': cols[13].text.strip(),
                            'running_position': cols[14].text.strip(),
                            'finish_time': cols[15].text.strip() if len(cols) > 15 else '',
                            'body_weight': cols[16].text.strip() if len(cols) > 16 else '',
                            'gear': cols[17].text.strip() if len(cols) > 17 else ''
                        }
                        race_history.append(race_record)

            return {
                'basic_info': basic_info,
                'race_history': race_history
            }
            
        except Exception as e:
            logging.error(f"获取马匹资料失败 - {horse_id}: {str(e)}")
            return None

    def process_race_day(self, race_date):
        """处理一个赛马日的所有数据"""
        race_data = []
        soup = self.get_race_results(race_date)
        
        if not soup:
            return None

        races = soup.find_all('div', {'class': 'f_fs13'})
        
        for race in races:
            try:
                race_info = {
                    'race_no': race.find('td', string=lambda x: x and '第' in x).text.strip() if race.find('td', string=lambda x: x and '第' in x) else 'Unknown',
                    'horses': []
                }
                
                horse_rows = race.find_all('tr', {'class': 'f_fs13'})
                for horse_row in horse_rows:
                    horse_info = self.parse_horse_info(horse_row)
                    if horse_info:
                        horse_id = f"HK_2023_{horse_info['horse_no']}"
                        horse_profile = self.get_horse_profile(horse_id)
                        if horse_profile:
                            horse_info.update(horse_profile)
                        race_info['horses'].append(horse_info)
                
                if race_info['horses']:
                    race_data.append(race_info)
                    
            except Exception as e:
                logging.error(f"处理赛事数据时出错: {str(e)}")
                continue
        
        return race_data

def main():
    scraper = HorseRacingScraper()
    
    # 设置要抓取的日期
    race_date = datetime.strptime('2024-01-01', '%Y-%m-%d')
    
    # 处理赛事数据
    race_data = scraper.process_race_day(race_date)
    
    if race_data:
        # 保存数据到YAML文件
        filename = f'race_data_{race_date.strftime("%Y%m%d")}.yaml'
        scraper.save_to_yaml(race_data, filename)

if __name__ == "__main__":
    main() 