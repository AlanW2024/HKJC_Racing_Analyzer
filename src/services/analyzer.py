from typing import List, Dict, Any
import pandas as pd
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class AnalysisResult:
    """分析结果数据结构"""
    date: str
    jockey: str
    total_races: int
    wins: int
    win_rate: float
    avg_position: float

class RaceAnalyzer:
    def analyze_races(self, race_data: List[Any]) -> List[Dict[str, Any]]:
        """分析赛事数据"""
        try:
            # 检查输入数据
            logger.info(f"开始分析 {len(race_data)} 条赛事数据")
            
            # 转换 SQLAlchemy 对象到字典列表
            data = []
            for race in race_data:
                data.append({
                    'race_date': race.race_date,
                    'jockey': race.jockey,
                    'race_id': race.race_id,
                    'finish_position': race.finish_position
                })
            
            logger.info(f"转换后数据条数: {len(data)}")
            
            # 转换为 DataFrame
            df = pd.DataFrame(data)
            logger.info(f"唯一骑师数量: {df['jockey'].nunique()}")
            
            # 按骑师分组分析
            jockey_stats = df.groupby('jockey').agg({
                'race_id': 'count',  # 总赛事数
                'finish_position': ['mean', lambda x: (x == 1).mean() * 100]  # 平均名次和胜率
            }).round(2)
            
            # 重命名列
            jockey_stats.columns = ['total_races', 'avg_position', 'win_rate']
            
            # 计算胜场数
            wins = df[df['finish_position'] == 1]['jockey'].value_counts()
            jockey_stats['wins'] = wins
            
            # 重置索引
            jockey_stats = jockey_stats.reset_index()
            
            # 转换为字典列表
            results = []
            for _, row in jockey_stats.iterrows():
                result = AnalysisResult(
                    date=df['race_date'].iloc[0],
                    jockey=row['jockey'],
                    total_races=int(row['total_races']),
                    wins=int(row['wins']) if not pd.isna(row['wins']) else 0,
                    win_rate=float(row['win_rate']),
                    avg_position=float(row['avg_position'])
                )
                results.append(result.__dict__)
            
            logger.info(f"分析完成，生成 {len(results)} 条骑师统计")
            return results
            
        except Exception as e:
            logger.error(f"分析数据时出错: {e}")
            logger.exception(e)  # 打印详细错误信息
            return [] 

    def analyze_yearly_stats(self, all_results: List[Dict]) -> Dict[str, Any]:
        """分析年度统计数据"""
        try:
            df = pd.DataFrame(all_results)
            
            # 确保赔率列为数值类型
            df['odds'] = pd.to_numeric(df['odds'], errors='coerce')
            
            # 骑师基础统计
            jockey_stats = df.groupby('jockey').agg({
                'race_id': 'count',  # 总赛事
                'finish_position': ['mean', lambda x: (x == 1).sum(), lambda x: (x == 1).mean() * 100]
            }).round(2)
            
            # 重命名列
            jockey_stats.columns = ['total_races', 'avg_position', 'total_wins', 'win_rate']
            jockey_stats = jockey_stats.reset_index()
            
            # 计算赔率分析
            odds_analysis = {
                'overall': {
                    'avg_odds': float(df['odds'].mean()),
                    'max_odds': float(df['odds'].max()),
                    'min_odds': float(df['odds'].min())
                },
                'winners': {
                    'avg_winning_odds': float(df[df['finish_position'] == 1]['odds'].mean()),
                    'highest_odds_winner': float(df[df['finish_position'] == 1]['odds'].max()),
                    'lowest_odds_winner': float(df[df['finish_position'] == 1]['odds'].min())
                },
                # 添加高赔率获胜统计
                'upset_wins': df[
                    (df['finish_position'] == 1) & 
                    (df['odds'] > df[df['finish_position'] == 1]['odds'].quantile(0.75))
                ].sort_values('odds', ascending=False).head().to_dict('records')
            }
            
            # 骑师赔率分析
            jockey_odds = df.groupby('jockey').agg({
                'odds': ['mean', 'min', 'max'],
                'finish_position': lambda x: (x == 1).mean() * 100
            }).round(2)
            
            # 重置索引并重命名列
            jockey_odds.columns = ['mean', 'min', 'max', 'win_rate'] 
            jockey_odds = jockey_odds.reset_index()
            
            odds_analysis['jockey_odds'] = jockey_odds.to_dict('records')
            
            return {
                'summary': {
                    'total_races': len(df),
                    'total_race_days': df['race_date'].nunique(),
                    'active_jockeys': df['jockey'].nunique(),
                    'avg_races_per_day': round(len(df) / df['race_date'].nunique(), 2),
                    'avg_odds': odds_analysis['overall']['avg_odds'],
                    'avg_winning_odds': odds_analysis['winners']['avg_winning_odds']
                },
                'odds_analysis': odds_analysis,
                'jockey_stats': jockey_stats.to_dict('records'),
                'top_jockeys': jockey_stats.nlargest(5, 'win_rate').to_dict('records'),
                'most_active': jockey_stats.nlargest(5, 'total_races').to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"年度统计分析出错: {e}")
            return {} 