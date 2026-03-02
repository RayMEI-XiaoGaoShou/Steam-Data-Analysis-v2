"""
可视化模块
使用 Plotly 创建交互式图表
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import List, Dict, Optional

from plotly.subplots import make_subplots
# 四象限颜色配置
QUADRANT_COLORS = {
    "千里马": "#F57C00",      # 橙色
    "潜力小子": "#FFC107",    # 黄色
    "问题小子": "#4CAF50",    # 绿色
    "彻底凉凉": "#2196F3",    # 蓝色
}


def create_tag_overview_chart(
    tag_stats: pd.DataFrame,
    selected_tags: List[str],
    global_avg_rate: float,
    global_avg_reviews: float,
) -> go.Figure:
    """
    创建综合分析图：Tags 的散点分布
    
    Args:
        tag_stats: Tag 统计数据 DataFrame
        selected_tags: 要显示的 Tag 列表
        global_avg_rate: 全局平均好评率
        global_avg_reviews: 全局平均评论数
    
    Returns:
        Plotly Figure 对象
    """
    # 筛选选中的 Tags
    df = tag_stats[tag_stats['tag'].isin(selected_tags)].copy()
    
    if df.empty:
        return create_empty_chart("请选择至少一个 Tag")
    
    # 分类到四象限
    df['quadrant'] = df.apply(
        lambda row: classify_quadrant(
            row['avg_positive_rate'],
            row['avg_reviews'],
            global_avg_rate,
            global_avg_reviews
        ),
        axis=1
    )
    df['color'] = [QUADRANT_COLORS.get(quadrant_name, '#666') for quadrant_name in df['quadrant']]
    
    # 创建散点图
    fig = go.Figure()
    
    # 按象限添加散点
    for quadrant, color in QUADRANT_COLORS.items():
        quad_df = df[df['quadrant'] == quadrant].copy()
        if len(quad_df) > 0:
            fig.add_trace(go.Scatter(
                x=quad_df['avg_reviews'],
                y=quad_df['avg_positive_rate'],
                mode='markers+text',
                name=quadrant,
                text=quad_df['tag'],
                textposition='top center',
                textfont=dict(size=10),
                marker=dict(
                    size=12,
                    color=color,
                    line=dict(width=1, color='white')
                ),
                hovertemplate=(
                    '<b>%{text}</b><br>'
                    '平均好评率: %{y:.1f}%<br>'
                    '平均评论数: %{x:,.0f}<br>'
                    '游戏数: %{customdata}<extra></extra>'
                ),
                customdata=quad_df['game_count']
            ))
    
    # 添加平均线
    fig.add_hline(
        y=global_avg_rate, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均好评率 {global_avg_rate:.1f}%",
        annotation_position="right"
    )
    fig.add_vline(
        x=global_avg_reviews, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均评论数 {global_avg_reviews:,.0f}",
        annotation_position="top"
    )
    
    # 布局设置
    fig.update_layout(
        title=dict(
            text="Tags 综合分析：好评率 vs 评论数",
            font=dict(size=18)
        ),
        xaxis=dict(
            title="平均评论数",
            type="log",  # 使用对数刻度
            gridcolor='lightgray',
        ),
        yaxis=dict(
            title="平均好评率 (%)",
            range=[60, 100],
            gridcolor='lightgray',
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        height=600,
        margin=dict(r=200)  # 为右侧统计预留空间
    )
    
    return fig


def create_single_tag_chart(
    games_df: pd.DataFrame,
    tag: str,
    avg_rate: float,
    avg_reviews: float,
) -> go.Figure:
    """
    创建具体分析图：单个 Tag 下的游戏散点分布
    
    Args:
        games_df: 该 Tag 下的游戏 DataFrame
        tag: Tag 名称
        avg_rate: 该 Tag 的平均好评率
        avg_reviews: 该 Tag 的平均评论数
    
    Returns:
        Plotly Figure 对象
    """
    if games_df.empty:
        return create_empty_chart(f"没有找到带有 \"{tag}\" 标签的游戏")
    
    df = games_df.copy()
    
    # 分类到四象限
    df['quadrant'] = df.apply(
        lambda row: classify_quadrant(
            row['positive_rate'],
            row['reviews'],
            avg_rate,
            avg_reviews
        ),
        axis=1
    )
    
    # 创建散点图
    fig = go.Figure()
    
    for quadrant, color in QUADRANT_COLORS.items():
        quad_df = df[df['quadrant'] == quadrant].copy()
        if len(quad_df) > 0:
            fig.add_trace(go.Scatter(
                x=quad_df['reviews'],
                y=quad_df['positive_rate'],
                mode='markers',
                name=quadrant,
                marker=dict(
                    size=10,
                    color=color,
                    opacity=0.7,
                    line=dict(width=1, color='white')
                ),
                hovertemplate=(
                    '<b>%{customdata}</b><br>'
                    '好评率: %{y}%<br>'
                    '评论数: %{x:,}<extra></extra>'
                ),
                customdata=quad_df['name']
            ))
    
    # 添加平均线
    fig.add_hline(
        y=avg_rate, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均好评率 {avg_rate:.1f}%",
        annotation_position="right"
    )
    fig.add_vline(
        x=avg_reviews, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均评论数 {avg_reviews:,.0f}",
        annotation_position="top"
    )
    
    # 布局设置
    fig.update_layout(
        title=dict(
            text=f"「{tag}」标签游戏分布 ({len(df)} 款游戏)",
            font=dict(size=18)
        ),
        xaxis=dict(
            title="评论数",
            type="log",
            gridcolor='lightgray',
        ),
        yaxis=dict(
            title="好评率 (%)",
            range=[0, 105],
            gridcolor='lightgray',
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        height=600,
    )
    
    return fig


def create_multi_tags_chart(
    games_df: pd.DataFrame,
    tags: List[str],
    avg_rate: float,
    avg_reviews: float,
) -> go.Figure:
    """
    创建多 Tags 分析图：多个 Tags 交集的游戏散点分布
    
    Args:
        games_df: 同时包含多个 Tags 的游戏 DataFrame
        tags: Tag 名称列表
        avg_rate: 平均好评率
        avg_reviews: 平均评论数
    
    Returns:
        Plotly Figure 对象
    """
    tags_str = " + ".join(tags)
    
    if games_df.empty or len(games_df) < 3:
        return create_empty_chart(f"「{tags_str}」组合的游戏数量少于 3 款，数据样本过小")
    
    df = games_df.copy()
    
    # 分类到四象限
    df['quadrant'] = df.apply(
        lambda row: classify_quadrant(
            row['positive_rate'],
            row['reviews'],
            avg_rate,
            avg_reviews
        ),
        axis=1
    )
    
    # 创建散点图
    fig = go.Figure()
    
    for quadrant, color in QUADRANT_COLORS.items():
        quad_df = df[df['quadrant'] == quadrant].copy()
        if len(quad_df) > 0:
            fig.add_trace(go.Scatter(
                x=quad_df['reviews'],
                y=quad_df['positive_rate'],
                mode='markers',
                name=quadrant,
                marker=dict(
                    size=10,
                    color=color,
                    opacity=0.7,
                    line=dict(width=1, color='white')
                ),
                hovertemplate=(
                    '<b>%{customdata}</b><br>'
                    '好评率: %{y}%<br>'
                    '评论数: %{x:,}<extra></extra>'
                ),
                customdata=quad_df['name']
            ))
    
    # 添加平均线
    fig.add_hline(
        y=avg_rate, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均好评率 {avg_rate:.1f}%",
        annotation_position="right"
    )
    fig.add_vline(
        x=avg_reviews, 
        line_dash="dash", 
        line_color="gray",
        annotation_text=f"平均评论数 {avg_reviews:,.0f}",
        annotation_position="top"
    )
    
    # 布局设置
    fig.update_layout(
        title=dict(
            text=f"「{tags_str}」组合游戏分布 ({len(df)} 款游戏)",
            font=dict(size=18)
        ),
        xaxis=dict(
            title="评论数",
            type="log",
            gridcolor='lightgray',
        ),
        yaxis=dict(
            title="好评率 (%)",
            range=[0, 105],
            gridcolor='lightgray',
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        plot_bgcolor='white',
        height=600,
    )
    
    return fig


def create_empty_chart(message: str) -> go.Figure:
    """创建空图表并显示提示信息"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=16, color="gray")
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor='white',
        height=400
    )
    return fig


def classify_quadrant(
    positive_rate: float, 
    reviews: float, 
    avg_positive_rate: float, 
    avg_reviews: float
) -> str:
    """分类到四象限"""
    high_rate = positive_rate >= avg_positive_rate
    high_reviews = reviews >= avg_reviews
    
    if high_rate and high_reviews:
        return "千里马"
    elif high_rate and not high_reviews:
        return "潜力小子"
    elif not high_rate and high_reviews:
        return "问题小子"
    else:
        return "彻底凉凉"


def create_quadrant_stats_html(stats: Dict[str, Dict[str, float]]) -> str:
    """
    创建四象限统计的 HTML 显示

    Args:
        stats: 四象限统计数据

    Returns:
        HTML 字符串
    """
    html_parts = []

    for quadrant, data in stats.items():
        color = QUADRANT_COLORS.get(quadrant, "#666")
        count = data.get('count', 0)
        percentage = data.get('percentage', 0)

        html_parts.append(f"""
        <div style="
            background-color: {color}; 
            color: white; 
            padding: 10px 15px; 
            border-radius: 8px; 
            margin-bottom: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        ">
            <div style="font-weight: bold; font-size: 14px;">{quadrant}</div>
            <div style="font-size: 20px; font-weight: bold;">{count} 个</div>
            <div style="font-size: 12px; opacity: 0.9;">占 {percentage}%</div>
        </div>
        """)

    return "\n".join(html_parts)



def create_tag_lift_chart(
    lift_data: pd.DataFrame,
    top_n: int = 20
) -> go.Figure:
    """
    创建 Tag Lift 分析图：水平条形图
    """
    if lift_data is None or lift_data.empty:
        return create_empty_chart("没有足够的 Lift 数据")
        
    if 'tag' not in lift_data.columns or 'lift' not in lift_data.columns:
        return create_empty_chart("数据格式错误：缺少 tag 或 lift 列")
        
    df = lift_data.nlargest(top_n, 'lift').sort_values('lift', ascending=True)
    
    fig = px.bar(
        df,
        x='lift',
        y='tag',
        orientation='h',
        title=f"TAG LIFT ANALYSIS // TOP {top_n}",
        color='lift',
        color_continuous_scale=px.colors.sequential.Plasma,
        text='lift'
    )
    
    fig.update_traces(
        texttemplate='%{text:.2f}', 
        textposition='outside',
        marker_line_color='black',
        marker_line_width=1.5,
        opacity=0.9
    )
    
    fig.update_layout(
        font=dict(family="Courier New, monospace", size=12, color="#2c3e50"),
        title=dict(font=dict(size=24, family="Impact, sans-serif"), x=0.05),
        xaxis_title="LIFT SCORE",
        yaxis_title="",
        plot_bgcolor='rgba(250, 250, 250, 0.8)',
        paper_bgcolor='rgba(255, 255, 255, 1)',
        height=max(500, len(df) * 30),
        margin=dict(l=150, r=50, t=80, b=50),
        xaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)', gridwidth=2),
        yaxis=dict(showgrid=False)
    )
    
    return fig


def create_tag_synergy_chart(
    synergy_data: pd.DataFrame,
    chart_type: str = 'bubble'
) -> go.Figure:
    """
    创建 Tag 组合协同效应图：热力图或气泡图
    """
    if synergy_data is None or synergy_data.empty:
        return create_empty_chart("没有足够的协同效应数据")
        
    if 'tag1' not in synergy_data.columns or 'tag2' not in synergy_data.columns or 'synergy_score' not in synergy_data.columns:
        has_legacy_columns = {'tag_a', 'tag_b', 'synergy_ratio'}.issubset(synergy_data.columns)
        if not has_legacy_columns:
            return create_empty_chart("数据格式错误：缺少 tag1, tag2 或 synergy_score 列")
        synergy_data = synergy_data.rename(columns={
            'tag_a': 'tag1',
            'tag_b': 'tag2',
            'synergy_ratio': 'synergy_score',
        })
        
    if chart_type == 'heatmap':
        pivot_df = synergy_data.pivot(index='tag1', columns='tag2', values='synergy_score')
        fig = px.imshow(
            pivot_df,
            labels=dict(x="TAG 2", y="TAG 1", color="SYNERGY"),
            x=pivot_df.columns,
            y=pivot_df.index,
            color_continuous_scale='Magma',
            title="TAG COMBO SYNERGY // HEATMAP"
        )
    else:
        df = synergy_data[synergy_data['tag1'] != synergy_data['tag2']].copy()
        if df.empty:
            df = synergy_data
            
        df['abs_score'] = [abs(float(score)) if pd.notna(score) else 0.0 for score in df['synergy_score']]
        
        fig = px.scatter(
            df,
            x='tag1',
            y='tag2',
            size='abs_score',
            color='synergy_score',
            color_continuous_scale='RdBu_r',
            color_continuous_midpoint=0,
            title="TAG COMBO SYNERGY // BUBBLE MATRIX",
            size_max=45,
            hover_data=['synergy_score']
        )
        
        fig.update_traces(
            marker=dict(line=dict(width=1, color='DarkSlateGrey')),
            selector=dict(mode='markers')
        )
        
    fig.update_layout(
        font=dict(family="Courier New, monospace", size=11),
        title=dict(font=dict(size=24, family="Impact, sans-serif"), x=0.05),
        plot_bgcolor='rgba(10, 10, 10, 0.03)',
        paper_bgcolor='white',
        height=800,
        width=800,
        xaxis=dict(tickangle=45, showgrid=True, gridcolor='rgba(0,0,0,0.05)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)'),
        margin=dict(l=100, r=50, t=100, b=100)
    )
    
    return fig


def create_time_trend_chart(
    trend_data: pd.DataFrame
) -> go.Figure:
    """
    创建时间趋势图：双轴折线/柱状图 (发布年份 vs 质量/数量)
    """
    if trend_data is None or trend_data.empty:
        return create_empty_chart("没有足够的时间趋势数据")
        
    if 'year' not in trend_data.columns or 'game_count' not in trend_data.columns or 'avg_positive_rate' not in trend_data.columns:
        return create_empty_chart("数据格式错误：缺少 year, game_count 或 avg_positive_rate 列")
        
    df = trend_data.sort_values('year')
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(
            x=df['year'],
            y=df['game_count'],
            name="RELEASE VOLUME",
            marker=dict(
                color='rgba(40, 40, 40, 0.8)',
                line=dict(color='black', width=1)
            ),
            opacity=0.6
        ),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['year'],
            y=df['avg_positive_rate'],
            name="QUALITY SCORE",
            mode='lines+markers',
            line=dict(color='#FF3366', width=4, shape='spline'),
            marker=dict(
                size=12, 
                color='white',
                line=dict(color='#FF3366', width=3),
                symbol='diamond'
            )
        ),
        secondary_y=True,
    )
    
    fig.update_layout(
        font=dict(family="Courier New, monospace"),
        title=dict(
            text="TEMPORAL EVOLUTION // VOLUME VS QUALITY",
            font=dict(size=24, family="Impact, sans-serif"),
            x=0.05
        ),
        plot_bgcolor='rgba(250, 250, 250, 1)',
        paper_bgcolor='white',
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="right",
            x=1,
            font=dict(family="Impact, sans-serif", size=14)
        ),
        margin=dict(l=50, r=50, t=100, b=50)
    )
    
    fig.update_xaxes(
        title_text="RELEASE YEAR", 
        gridcolor='rgba(0,0,0,0.05)',
        tickfont=dict(family="Impact, sans-serif", size=14)
    )
    
    fig.update_yaxes(
        title_text="VOLUME (GAMES)", 
        secondary_y=False, 
        gridcolor='rgba(0,0,0,0.05)',
        tickfont=dict(family="Impact, sans-serif", size=14)
    )
    
    fig.update_yaxes(
        title_text="QUALITY (%)", 
        secondary_y=True, 
        range=[max(0, df['avg_positive_rate'].min() - 10), min(100, df['avg_positive_rate'].max() + 10)],
        showgrid=False,
        tickfont=dict(family="Impact, sans-serif", size=14, color='#FF3366'),
        titlefont=dict(color='#FF3366')
    )
    
    return fig
