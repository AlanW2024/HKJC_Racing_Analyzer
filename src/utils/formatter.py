def format_analysis_results(results):
    """格式化分析結果"""
    output = []
    
    # 添加分隔線
    output.append("\n" + "="*50 + "\n")
    
    # 總結信息
    output.append("比賽總結:")
    output.append(f"總場次: {results['total_races']}")
    output.append(f"參賽馬匹: {results['total_horses']}")
    output.append(f"騎師人數: {results['total_jockeys']}")
    
    # 騎師表現TOP5
    output.append("\n最佳騎師表現:")
    jockeys = sorted(
        results['jockey_analysis'].items(), 
        key=lambda x: x[1]['win_rate'],
        reverse=True
    )[:5]
    
    for name, stats in jockeys:
        output.append(
            f"{name}: 勝率{stats['win_rate']}%, "
            f"出賽{stats['total_races']}次"
        )
    
    return "\n".join(output) 