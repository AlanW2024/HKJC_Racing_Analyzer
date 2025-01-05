from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import asyncio
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class RaceData:
    """比赛数据结构"""
    race_id: str
    race_date: str
    horse_no: str
    horse_name: str
    draw: int
    finish_position: int
    jockey: str
    trainer: str
    finish_time: str
    odds: float
    distance: int
    race_info: str

class RaceScraper:
    def __init__(self, config: Dict):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx"
        self.config = config
        self.is_initialized = False  # 添加初始化标志
        self._reset_browser_instances()

    def _reset_browser_instances(self):
        """重置浏览器实例"""
        self.playwright = None
        self.browser = None
        self.context = None
        self.is_initialized = False  # 重置初始化标志

    async def init(self):
        """初始化浏览器"""
        try:
            await self._init_browser()
            self.is_initialized = True  # 设置初始化标志
            logger.info("爬虫初始化成功")
        except Exception as e:
            logger.error(f"爬虫初始化失败: {e}")
            self.is_initialized = False
            raise

    async def close(self):
        """关闭浏览器"""
        try:
            await self._cleanup()
            self.is_initialized = False  # 重置初始化标志
        except Exception as e:
            logger.error(f"关闭爬虫时出错: {e}")
            raise

    async def _init_browser(self) -> None:
        """初始化浏览器"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.config.get('HEADLESS', True),
                args=['--disable-dev-shm-usage']
            )
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            logger.info("浏览器初始化成功")
        except Exception as e:
            logger.error(f"浏览器初始化失败: {e}")
            await self._cleanup()
            raise

    async def _cleanup(self) -> None:
        """清理资源"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            await asyncio.sleep(0.1)
            self._reset_browser_instances()
            logger.info("浏览器资源已清理")
        except Exception as e:
            logger.error(f"清理浏览器资源时出错: {e}")

    @staticmethod
    def _parse_race_info(info_text: str) -> tuple[str, int]:
        """解析比赛信息"""
        try:
            race_id = info_text.split()[2].strip("()")
            distance = int(''.join(filter(str.isdigit, info_text.split('-')[1].strip())))
            return race_id, distance
        except Exception:
            return "0", 0

    @staticmethod
    def _parse_finish_position(text: str) -> int:
        """解析完赛位置"""
        if not text or text in ["WV", "---", "DISQ", "DNF", "PU", "WX", ""]:
            return 99
        try:
            return int(text)
        except ValueError:
            logger.warning(f"无效名次: {text}，设置为 99")
            return 99

    async def scrape_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """抓取指定日期范围的赛事数据"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        all_data = []
        
        current = start
        while current <= end:
            date_str = current.strftime("%Y/%m/%d")
            races = await self.scrape_single_date(date_str)
            if races:
                all_data.extend(races)
            current += timedelta(days=1)
            
        return all_data

    async def scrape_single_date(self, date: str) -> List[Dict[str, Any]]:
        """抓取单个日期的赛事数据"""
        self.current_date = date  # 保存當前日期
        page = await self.context.new_page()
        all_data = []
        
        try:
            # 檢查該日期是否有賽事
            check_url = (
                "https://racing.hkjc.com/racing/information/Chinese/Racing/"
                f"LocalResults.aspx?RaceDate={date}&Racecourse=ST&RaceNo=1"
            )
            print(f"正在检查: {check_url}")
            
            print(f"正在加載頁面...")
            await page.goto(check_url, timeout=30000, wait_until='networkidle')
            print(f"等待DOM加載完成...")
            await page.wait_for_load_state('domcontentloaded')
            
            # 检查是否有赛事
            print(f"檢查是否有賽事...")
            has_race = await page.is_visible("table.table_bd.draggable")
            if not has_race:
                print(f"日期 {date} 没有赛事")
                return []
            
            # 假设每日最多12场比赛
            for race_no in range(1, 13):
                try:
                    print(f"\n開始處理第 {race_no} 場比賽...")
                    race_url = (
                        "https://racing.hkjc.com/racing/information/Chinese/Racing/"
                        f"LocalResults.aspx?RaceDate={date}&Racecourse=ST&RaceNo={race_no}"
                    )
                    print(f"訪問URL: {race_url}")
                    await page.goto(race_url, timeout=30000)
                    
                    # 检查该场次是否存在
                    print(f"檢查比賽是否存在...")
                    if not await page.is_visible("table.table_bd.draggable"):
                        print(f"第 {race_no} 场比赛不存在")
                        break
                    
                    print(f"處理第 {race_no} 場比賽數據...")
                    
                    # 获取赛事信息
                    print(f"獲取賽事信息...")
                    race_info = await page.query_selector(".race_tab .f_title")
                    if not race_info:
                        print("使用備用選擇器...")
                        race_info = await page.query_selector(".race_tab td:has-text('第')")
                    race_info_text = await race_info.inner_text() if race_info else "N/A"
                    
                    # 获取赛事距离
                    print(f"獲取賽事距離...")
                    distance_element = await page.query_selector(".race_tab td:has-text('米')")
                    distance = await distance_element.inner_text() if distance_element else "N/A"
                    distance = distance.strip() if distance else "N/A"
                    print(f"賽事距離: {distance}")
                    
                    # 将距离信息添加到 race_info_text
                    race_info_text = f"{race_info_text} {distance}"
                    print(f"完整賽事信息: {race_info_text}")
                    
                    self.current_race_info = race_info_text
                    
                    # 获取所有行
                    rows = await page.query_selector_all("table.table_bd.draggable tr:not(.bg_blue):not(.bg_gold)")
                    
                    # 处理每一行数据
                    for row in rows:
                        race_data = await self._extract_race_data(row)
                        if race_data:
                            all_data.append(race_data)
                            
                except Exception as e:
                    print(f"處理第 {race_no} 場比賽時出錯: {e}")
                    continue
                
            print(f"成功解析 {len(all_data)} 条数据")
            return all_data
            
        except Exception as e:
            print(f"抓取数据时出错: {e}")
            return []
            
        finally:
            await page.close()

    async def _extract_race_data(self, row) -> Optional[Dict[str, Any]]:
        """从表格行中提取赛事数据"""
        try:
            cols = await row.query_selector_all("td")
            if len(cols) < 12:
                return None
            
            # 提取馬匹編號和名稱
            horse_name = (await cols[2].inner_text()).strip()
            horse_no = ""
            if "(" in horse_name and ")" in horse_name:
                horse_no = horse_name[horse_name.find("(")+1:horse_name.find(")")]
                horse_name = horse_name[:horse_name.find("(")].strip()
            
            # 提取名次
            rank_text = (await cols[1].inner_text()).strip()
            if not rank_text or rank_text in ["WV", "---", "DISQ", "DNF", "PU", "WX", ""]:
                finish_position = 99
            else:
                try:
                    finish_position = int(rank_text)
                except ValueError:
                    logger.warning(f"无效名次: {rank_text}，设置为 99")
                    finish_position = 99
            
            # 提取檔位
            draw = (await cols[0].inner_text()).strip()
            try:
                draw = int(draw)
            except ValueError:
                draw = 0
            
            # 提取賽事資訊
            race_info = self.current_race_info
            if race_info:
                # 只提取括号中的数字
                race_id = ''.join(filter(str.isdigit, race_info.split()[2]))
                distance = int(''.join(filter(str.isdigit, race_info.split('-')[1].strip())))
            else:
                race_id = "0"
                distance = 0
            
            # 提取賠率
            odds_text = (await cols[11].inner_text()).strip()
            try:
                odds = float(odds_text.replace('---', '0'))
            except ValueError:
                odds = 0.0
            
            return {
                "race_id": race_id,
                "race_date": self.current_date,
                "horse_no": horse_no,
                "horse_name": horse_name,
                "draw": draw,
                "finish_position": finish_position,
                "jockey": (await cols[3].inner_text()).strip(),
                "trainer": (await cols[4].inner_text()).strip(),
                "finish_time": (await cols[10].inner_text()).strip(),
                "odds": odds,
                "distance": distance,
                "race_info": race_info
            }
            
        except Exception as e:
            print(f"提取数据时出错: {e}")
            return None

    async def scrape_race_data(self, date: str):
        """爬取指定日期的赛马数据"""
        try:
            # 确保日期格式统一为 YYYY-MM-DD
            formatted_date = date.replace('/', '-')
            logger.info(f"处理日期: {formatted_date}")
            
            # 转换为爬取用的格式 YYYY/MM/DD
            scrape_date = formatted_date.replace('-', '/')
            race_data = await self.scrape_single_date(scrape_date)
            
            if race_data:
                # 确保所有数据使用统一的日期格式
                for item in race_data:
                    item['race_date'] = formatted_date
                    
                logger.info(f"成功爬取 {formatted_date} 的赛事数据，共 {len(race_data)} 条记录")
                return race_data
            else:
                logger.warning(f"日期 {formatted_date} 没有找到赛事数据")
                return []
            
        except Exception as e:
            logger.error(f"爬取 {date} 的数据时出错: {e}")
            return [] 