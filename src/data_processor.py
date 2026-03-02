"""
数据处理模块
加载和处理 Steam 游戏数据
"""

import pandas as pd
import streamlit as st
import importlib
from pathlib import Path
from typing import List, Tuple, Dict, Optional

# 数据文件路径
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_FILE = DATA_DIR / "bestSelling_games.csv"
SUPABASE_TABLE = "v_steam_games_v2"

# 四象限分类颜色
QUADRANT_COLORS = {
    "千里马": "#F57C00",      # 橙色 - 高好评率 + 高评论数
    "潜力小子": "#FFC107",    # 黄色 - 高好评率 + 低评论数  
    "问题小子": "#4CAF50",    # 绿色 - 低好评率 + 高评论数
    "彻底凉凉": "#2196F3",    # 蓝色 - 低好评率 + 低评论数
}


def load_data(min_reviews: int = 200) -> pd.DataFrame:
    """
    加载并清洗数据
    
    Args:
        min_reviews: 最小评论数阈值
    
    Returns:
        清洗后的 DataFrame
    """
    df_supabase = _load_data_from_supabase()
    if df_supabase is None:
        # CSV fallback for local/dev scenarios
        st.warning("⚠️ 无法连接到 Supabase (或未获取到数据)，正在使用本地 CSV 数据作为 fallback。如果有报错请查看右边通知。")
        df = pd.read_csv(DATA_FILE, encoding='latin-1')
        df = df.drop(columns=['Unnamed: 4', 'Unnamed: 5'], errors='ignore')
        df = df.rename(columns={
            'game_name': 'name',
            'reviews_like_rate': 'positive_rate',
            'all_reviews_number': 'reviews',
            'user_defined_tags': 'tags_str'
        })
    else:
        df = df_supabase
    
    # 标准化字段类型
    df['reviews'] = pd.to_numeric(df['reviews'], errors='coerce').fillna(0).astype(int)
    df['positive_rate'] = pd.to_numeric(df['positive_rate'], errors='coerce').fillna(0.0)
    # Supabase 数据一般是 0-1，小于等于 1 时转百分比，和旧页面口径保持一致
    if not df.empty and df['positive_rate'].max() <= 1.0:
        df['positive_rate'] = df['positive_rate'] * 100

    # 筛选评论数
    df = df[df['reviews'] >= min_reviews].copy()
    
    # 解析 Tags
    df['tags'] = df['tags_str'].apply(parse_tags)
    
    # 提取前5个 Tags
    for i in range(5):
        df[f'tag_{i+1}'] = df['tags'].apply(lambda x: x[i] if i < len(x) else '')
    
    return df


def _load_data_from_supabase() -> Optional[pd.DataFrame]:
    """Load data from Supabase if secrets and dependency are available.

    Returns None when Supabase is not configured or query fails.
    """
    try:
        supabase_module = importlib.import_module("supabase")
        create_client = getattr(supabase_module, "create_client")
    except Exception as e:
        st.error(f"Supabase 初始化异常 (import): {str(e)}")
        return None

    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception as e:
        st.error(f"Supabase 读取 Secrets 异常: {str(e)}")
        return None

    try:
        client = create_client(url, key)
        page_size = 1000
        offset = 0
        rows = []

        while True:
            resp = (
                client.table(SUPABASE_TABLE)
                .select("appid,game,review_count,positive_rate,tag1,tag2,tag3,tag4,tag5")
                .limit(page_size)
                .offset(offset)
                .execute()
            )
            batch = resp.data or []
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        if not rows:
            st.warning(f"Supabase 查询成功，但表 {SUPABASE_TABLE} 中没有数据返回。")
            return None

        df = pd.DataFrame(rows)
        df = df.rename(columns={
            'game': 'name',
            'review_count': 'reviews',
        })

        for i in range(1, 6):
            col = f'tag{i}'
            if col not in df.columns:
                df[col] = ''
            df[col] = df[col].fillna('').astype(str)

        df['tags_str'] = df.apply(
            lambda r: ', '.join([r['tag1'], r['tag2'], r['tag3'], r['tag4'], r['tag5']]).strip(', ').strip(),
            axis=1,
        )
        return df
    except Exception as e:
        st.error(f"Supabase 数据抓取或解析异常: {str(e)}")
        return None


def parse_tags(tags_str: str) -> List[str]:
    """解析 Tags 字符串为列表"""
    if pd.isna(tags_str) or not tags_str:
        return []
    
    # Tags 以逗号分隔
    tags = [tag.strip() for tag in str(tags_str).split(',')]
    return [tag for tag in tags if tag]  # 过滤空字符串


def get_all_tags(df: pd.DataFrame) -> List[str]:
    """
    获取所有唯一的 Tags，按出现频率排序
    
    Returns:
        按频率降序排列的 Tag 列表
    """
    tag_counts = {}
    
    for tags in df['tags']:
        for tag in tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    
    # 按频率排序
    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
    return [tag for tag, count in sorted_tags]


def get_top_tags(df: pd.DataFrame, n: int = 10) -> List[str]:
    """获取出现频率最高的 N 个 Tags"""
    return get_all_tags(df)[:n]


def calculate_global_stats(df: pd.DataFrame) -> Dict[str, float]:
    """
    计算全局统计数据
    
    Returns:
        包含平均好评率和平均评论数的字典
    """
    return {
        'avg_positive_rate': float(df['positive_rate'].mean()),
        'avg_reviews': float(df['reviews'].mean()),
        'total_games': float(len(df)),
    }


def calculate_tag_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算每个 Tag 的统计数据
    
    Returns:
        包含每个 Tag 的平均好评率、平均评论数、游戏数的 DataFrame
    """
    tag_stats = []
    
    for tag in get_all_tags(df):
        # 筛选包含该 Tag 的游戏
        mask = df['tags'].apply(lambda x: tag in x)
        tag_games = df[mask]
        
        if len(tag_games) > 0:
            tag_stats.append({
                'tag': tag,
                'avg_positive_rate': tag_games['positive_rate'].mean(),
                'avg_reviews': tag_games['reviews'].mean(),
                'game_count': len(tag_games),
            })
    
    return pd.DataFrame(tag_stats)


def classify_quadrant(
    positive_rate: float, 
    reviews: float, 
    avg_positive_rate: float, 
    avg_reviews: float
) -> str:
    """
    根据好评率和评论数分类到四象限
    
    Args:
        positive_rate: 好评率
        reviews: 评论数
        avg_positive_rate: 平均好评率（阈值）
        avg_reviews: 平均评论数（阈值）
    
    Returns:
        象限名称
    """
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


def get_games_by_tag(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """获取包含指定 Tag 的所有游戏"""
    mask = df['tags'].apply(lambda x: tag in x)
    return df[mask].copy()


def get_games_by_tags(df: pd.DataFrame, tags: List[str]) -> pd.DataFrame:
    """获取同时包含多个 Tags 的游戏"""
    if not tags:
        return pd.DataFrame()
    
    mask = df['tags'].apply(lambda x: all(tag in x for tag in tags))
    return df[mask].copy()


def calculate_quadrant_stats(
    df: pd.DataFrame, 
    avg_positive_rate: float, 
    avg_reviews: float
) -> Dict[str, Dict]:
    """
    计算四象限的统计数据
    
    Returns:
        每个象限的游戏数量和占比
    """
    total = len(df)
    if total == 0:
        return {}
    
    df = df.copy()
    df['quadrant'] = df.apply(
        lambda row: classify_quadrant(
            row['positive_rate'], 
            row['reviews'], 
            avg_positive_rate, 
            avg_reviews
        ), 
        axis=1
    )
    
    stats = {}
    for quadrant in QUADRANT_COLORS.keys():
        count = (df['quadrant'] == quadrant).sum()
        stats[quadrant] = {
            'count': count,
            'percentage': round(count / total * 100, 1) if total > 0 else 0
        }
    
    return stats


if __name__ == "__main__":
    # 测试
    print("Loading data...")
    df = load_data(min_reviews=200)
    print(f"Loaded {len(df)} games")
    
    print("\nGlobal stats:")
    stats = calculate_global_stats(df)
    print(f"  Avg positive rate: {stats['avg_positive_rate']:.1f}%")
    print(f"  Avg reviews: {stats['avg_reviews']:.0f}")
    
    print("\nTop 10 tags:")
    for tag in get_top_tags(df, 10):
        print(f"  - {tag}")
    
    print("\nTag stats (top 5):")
    tag_stats = calculate_tag_stats(df)
    print(tag_stats.head().to_string())
