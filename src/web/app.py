from flask import Flask, render_template
from src.services.analyzer import RaceAnalyzer
from src.services.visualizer import RaceVisualizer
from src.services.storage import DataStorage

app = Flask(__name__)
storage = DataStorage()
analyzer = RaceAnalyzer()
visualizer = RaceVisualizer()

@app.route('/')
def index():
    # 獲取最新分析結果
    stats = storage.get_jockey_stats()
    
    # 生成圖表
    performance_chart = visualizer.plot_jockey_performance(stats)
    
    return render_template(
        'index.html',
        performance_chart=performance_chart,
        stats=stats
    )

@app.route('/analysis')
def analysis():
    return render_template('analysis.html') 