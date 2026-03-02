"""
Steam 游戏数据分析 Dashboard
主应用入口
"""

import streamlit as st
import sys
from pathlib import Path

# 添加 src 目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent))

from data_processor import (
    load_data, 
    get_all_tags, 
    get_top_tags,
    calculate_global_stats,
    calculate_tag_stats,
    get_games_by_tag,
    get_games_by_tags,
    calculate_quadrant_stats,
    QUADRANT_COLORS
)
from charts import (
    create_tag_overview_chart,
    create_single_tag_chart,
    create_multi_tags_chart,
    create_quadrant_stats_html
)

# 页面配置
st.set_page_config(
    page_title="Steam 游戏数据分析",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义 CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1a1a2e;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .stats-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 10px;
    }
    .stats-number {
        font-size: 2rem;
        font-weight: bold;
    }
    .stats-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .quadrant-box {
        padding: 10px 15px;
        border-radius: 8px;
        margin-bottom: 8px;
        color: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_cached_data(min_reviews: int):
    """缓存数据加载"""
    return load_data(min_reviews)


@st.cache_data
def get_cached_tag_stats(min_reviews: int):
    """缓存 Tag 统计数据"""
    df = load_cached_data(min_reviews)
    return calculate_tag_stats(df)


def main():
    # 标题
    st.markdown('<div class="main-header">🎮 Steam 游戏数据分析 Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">分析 Tags 与好评率、评论数之间的关联</div>', unsafe_allow_html=True)
    
    # 侧边栏 - 全局设置
    with st.sidebar:
        st.header("⚙️ 设置")
        
        # 评论数阈值滑块
        min_reviews = st.slider(
            "最小评论数阈值",
            min_value=200,
            max_value=1000,
            value=200,
            step=50,
            help="只分析评论数大于此阈值的游戏"
        )
        
        st.divider()
        
        # 加载数据
        df = load_cached_data(min_reviews)
        global_stats = calculate_global_stats(df)
        
        # 显示数据概览
        st.subheader("📊 数据概览")
        st.metric("游戏总数", f"{global_stats['total_games']:,}")
        st.metric("平均好评率", f"{global_stats['avg_positive_rate']:.1f}%")
        st.metric("平均评论数", f"{global_stats['avg_reviews']:,.0f}")
        
        st.divider()
        
        # 图例说明
        st.subheader("📍 四象限说明")
        for name, color in QUADRANT_COLORS.items():
            if name == "千里马":
                desc = "高好评 + 高评论"
            elif name == "潜力小子":
                desc = "高好评 + 低评论"
            elif name == "问题小子":
                desc = "低好评 + 高评论"
            else:
                desc = "低好评 + 低评论"
            
            st.markdown(f"""
            <div style="
                background-color: {color}; 
                color: white; 
                padding: 8px 12px; 
                border-radius: 6px; 
                margin-bottom: 6px;
                font-size: 13px;
            ">
                <b>{name}</b><br>
                <span style="opacity: 0.9;">{desc}</span>
            </div>
            """, unsafe_allow_html=True)
    
    # 主内容区 - 使用 Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 综合分析", 
        "🔍 单 Tag 分析", 
        "🔗 多 Tags 分析",
        "📋 数据表格"
    ])
    
    # 获取数据
    df = load_cached_data(min_reviews)
    global_stats = calculate_global_stats(df)
    tag_stats = get_cached_tag_stats(min_reviews)
    all_tags = get_all_tags(df)
    
    # ==================== Tab 1: 综合分析 ====================
    with tab1:
        st.subheader("Tags 综合分析")
        st.markdown("每个圆点代表一个 Tag，位置表示该 Tag 下所有游戏的**平均好评率**和**平均评论数**。")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            st.markdown("**选择 Tags**")
            
            # 默认选择热门10个
            top_10 = get_top_tags(df, 10)
            
            # Tag 选择方式
            selection_mode = st.radio(
                "选择方式",
                ["热门 Tags", "自定义选择"],
                horizontal=True,
                label_visibility="collapsed"
            )
            
            if selection_mode == "热门 Tags":
                n_tags = st.slider("显示数量", 5, 20, 10)
                selected_tags = get_top_tags(df, n_tags)
            else:
                selected_tags = st.multiselect(
                    "选择 Tags",
                    options=all_tags,
                    default=top_10[:10],
                    help="可以搜索并选择多个 Tags"
                )
            
            st.markdown("---")
            
            # 显示四象限统计
            if selected_tags:
                filtered_stats = tag_stats[tag_stats['tag'].isin(selected_tags)]
                
                st.markdown("**四象限分布**")
                for quadrant, color in QUADRANT_COLORS.items():
                    # 计算该象限的 Tags
                    quad_tags = []
                    for _, row in filtered_stats.iterrows():
                        high_rate = row['avg_positive_rate'] >= global_stats['avg_positive_rate']
                        high_reviews = row['avg_reviews'] >= global_stats['avg_reviews']
                        
                        if quadrant == "千里马" and high_rate and high_reviews:
                            quad_tags.append(row['tag'])
                        elif quadrant == "潜力小子" and high_rate and not high_reviews:
                            quad_tags.append(row['tag'])
                        elif quadrant == "问题小子" and not high_rate and high_reviews:
                            quad_tags.append(row['tag'])
                        elif quadrant == "彻底凉凉" and not high_rate and not high_reviews:
                            quad_tags.append(row['tag'])
                    
                    with st.expander(f"{quadrant} ({len(quad_tags)})", expanded=False):
                        if quad_tags:
                            for tag in quad_tags:
                                st.markdown(f"• {tag}")
                        else:
                            st.markdown("*无*")
        
        with col1:
            if selected_tags:
                fig = create_tag_overview_chart(
                    tag_stats,
                    selected_tags,
                    global_stats['avg_positive_rate'],
                    global_stats['avg_reviews']
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("请在右侧选择至少一个 Tag")
    
    # ==================== Tab 2: 单 Tag 分析 ====================
    with tab2:
        st.subheader("单 Tag 详细分析")
        st.markdown("选择一个 Tag，查看该 Tag 下所有游戏的分布情况。")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            # Tag 选择
            selected_tag = st.selectbox(
                "选择 Tag",
                options=all_tags,
                index=0,
                help="选择要分析的 Tag"
            )
            
            if selected_tag:
                # 获取该 Tag 的游戏
                tag_games = get_games_by_tag(df, selected_tag)
                
                if not tag_games.empty:
                    tag_avg_rate = tag_games['positive_rate'].mean()
                    tag_avg_reviews = tag_games['reviews'].mean()
                    
                    st.markdown("---")
                    st.markdown("**Tag 统计**")
                    st.metric("游戏数量", len(tag_games))
                    st.metric("平均好评率", f"{tag_avg_rate:.1f}%")
                    st.metric("平均评论数", f"{tag_avg_reviews:,.0f}")
                    
                    st.markdown("---")
                    st.markdown("**四象限分布**")
                    
                    quad_stats = calculate_quadrant_stats(
                        tag_games, 
                        tag_avg_rate, 
                        tag_avg_reviews
                    )
                    
                    for quadrant, data in quad_stats.items():
                        color = QUADRANT_COLORS[quadrant]
                        st.markdown(f"""
                        <div style="
                            background-color: {color}; 
                            color: white; 
                            padding: 10px 12px; 
                            border-radius: 6px; 
                            margin-bottom: 6px;
                        ">
                            <div style="font-weight: bold;">{quadrant}</div>
                            <div>{data['count']} 个，占 {data['percentage']}%</div>
                        </div>
                        """, unsafe_allow_html=True)
        
        with col1:
            if selected_tag:
                tag_games = get_games_by_tag(df, selected_tag)
                
                if not tag_games.empty:
                    tag_avg_rate = tag_games['positive_rate'].mean()
                    tag_avg_reviews = tag_games['reviews'].mean()
                    
                    fig = create_single_tag_chart(
                        tag_games,
                        selected_tag,
                        tag_avg_rate,
                        tag_avg_reviews
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"没有找到带有「{selected_tag}」标签的游戏")
    
    # ==================== Tab 3: 多 Tags 分析 ====================
    with tab3:
        st.subheader("多 Tags 组合分析")
        st.markdown("选择 2-3 个 Tags，查看同时拥有这些 Tags 的游戏分布。")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            # 多 Tag 选择
            selected_multi_tags = st.multiselect(
                "选择 Tags (2-3个)",
                options=all_tags,
                default=[],
                max_selections=3,
                help="选择 2-3 个 Tags 进行组合分析"
            )
            
            if len(selected_multi_tags) >= 2:
                # 获取交集游戏
                multi_tag_games = get_games_by_tags(df, selected_multi_tags)
                
                st.markdown("---")
                st.markdown("**组合统计**")
                st.metric("匹配游戏数", len(multi_tag_games))
                
                if len(multi_tag_games) >= 3:
                    tag_avg_rate = multi_tag_games['positive_rate'].mean()
                    tag_avg_reviews = multi_tag_games['reviews'].mean()
                    
                    st.metric("平均好评率", f"{tag_avg_rate:.1f}%")
                    st.metric("平均评论数", f"{tag_avg_reviews:,.0f}")
                    
                    st.markdown("---")
                    st.markdown("**四象限分布**")
                    
                    quad_stats = calculate_quadrant_stats(
                        multi_tag_games, 
                        tag_avg_rate, 
                        tag_avg_reviews
                    )
                    
                    for quadrant, data in quad_stats.items():
                        color = QUADRANT_COLORS[quadrant]
                        st.markdown(f"""
                        <div style="
                            background-color: {color}; 
                            color: white; 
                            padding: 10px 12px; 
                            border-radius: 6px; 
                            margin-bottom: 6px;
                        ">
                            <div style="font-weight: bold;">{quadrant}</div>
                            <div>{data['count']} 个，占 {data['percentage']}%</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.warning("⚠️ 数据样本过小 (少于3款游戏)")
        
        with col1:
            if len(selected_multi_tags) < 2:
                st.info("请在右侧选择至少 2 个 Tags")
            else:
                multi_tag_games = get_games_by_tags(df, selected_multi_tags)
                
                if len(multi_tag_games) >= 3:
                    tag_avg_rate = multi_tag_games['positive_rate'].mean()
                    tag_avg_reviews = multi_tag_games['reviews'].mean()
                    
                    fig = create_multi_tags_chart(
                        multi_tag_games,
                        selected_multi_tags,
                        tag_avg_rate,
                        tag_avg_reviews
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error(f"「{' + '.join(selected_multi_tags)}」组合只有 {len(multi_tag_games)} 款游戏，数据样本过小（需要至少3款）")
    
    # ==================== Tab 4: 数据表格 ====================
    with tab4:
        st.subheader("游戏数据明细")
        
        # 筛选器
        col1, col2, col3 = st.columns(3)
        
        with col1:
            filter_tag = st.selectbox(
                "按 Tag 筛选",
                options=["全部"] + all_tags,
                index=0
            )
        
        with col2:
            min_rate = st.slider("最低好评率", 0, 100, 0)
        
        with col3:
            sort_by = st.selectbox(
                "排序方式",
                options=["评论数（高→低）", "评论数（低→高）", "好评率（高→低）", "好评率（低→高）"]
            )
        
        # 应用筛选
        display_df = df.copy()
        
        if filter_tag != "全部":
            display_df = get_games_by_tag(display_df, filter_tag)
        
        display_df = display_df[display_df['positive_rate'] >= min_rate]
        
        # 排序
        if sort_by == "评论数（高→低）":
            display_df = display_df.sort_values('reviews', ascending=False)
        elif sort_by == "评论数（低→高）":
            display_df = display_df.sort_values('reviews', ascending=True)
        elif sort_by == "好评率（高→低）":
            display_df = display_df.sort_values('positive_rate', ascending=False)
        else:
            display_df = display_df.sort_values('positive_rate', ascending=True)
        
        # 显示表格
        st.dataframe(
            display_df[['name', 'positive_rate', 'reviews', 'tag_1', 'tag_2', 'tag_3', 'tag_4', 'tag_5']].rename(columns={
                'name': '游戏名称',
                'positive_rate': '好评率 (%)',
                'reviews': '评论数',
                'tag_1': 'Tag 1',
                'tag_2': 'Tag 2',
                'tag_3': 'Tag 3',
                'tag_4': 'Tag 4',
                'tag_5': 'Tag 5',
            }),
            use_container_width=True,
            height=500
        )
        
        st.caption(f"共 {len(display_df)} 款游戏")


if __name__ == "__main__":
    main()
