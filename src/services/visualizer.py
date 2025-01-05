import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List

class RaceVisualizer:
    def plot_jockey_performance(self, stats: Dict):
        """繪製騎師表現圖表"""
        fig = px.bar(
            stats,
            x='jockey',
            y=['win_rate', 'place_rate'],
            title='騎師勝率分析',
            barmode='group'
        )
        return fig
        
    def plot_odds_distribution(self, data: List[Dict]):
        """繪製賠率分布圖"""
        fig = px.histogram(
            data,
            x='odds',
            nbins=50,
            title='賠率分布'
        )
        return fig 