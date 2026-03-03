"""
数据处理模块
加载和处理 Steam 游戏数据
"""

# pyright: reportAttributeAccessIssue=false,reportArgumentType=false,reportCallIssue=false,reportReturnType=false,reportIndexIssue=false,reportMissingTypeArgument=false

import pandas as pd
import streamlit as st
import importlib
import math
from itertools import combinations
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Set

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
    if 'tags_str' not in df.columns:
        df['tags_str'] = ''
    df['tags'] = df['tags_str'].apply(parse_tags)
    
    # 提取前5个 Tags
    for i in range(5):
        df[f'tag_{i+1}'] = df['tags'].apply(lambda x: x[i] if i < len(x) else '')

    # 解析发布时间，供时间趋势分析使用
    df = _add_release_time_features(df)
    
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
                client.table(SUPABASE_TABLE).select("appid,game,review_count,positive_rate,tags,release_date").offset(offset).execute()
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
            df[col] = df['tags'].apply(lambda x: x[i-1] if isinstance(x, list) and len(x) >= i else '')

        df['tags_str'] = df.apply(
            lambda r: ', '.join([r['tag1'], r['tag2'], r['tag3'], r['tag4'], r['tag5']]).strip(', ').strip(),
            axis=1,
        )
        return df
    except Exception as e:
        st.error(f"Supabase 数据抓取或解析异常: {str(e)}")
        return None


def parse_tags(tags_str: str) -> List[str]:
    """
    解析 Tags 字符串为去重后的列表。

    处理规则：
        1. 以逗号分隔字符串；
        2. 去除首尾空格和空值；
        3. 在保留原顺序的前提下去重。

    Args:
        tags_str: 原始标签字符串，通常为逗号分隔文本。

    Returns:
        清洗后的 Tag 列表。
    """
    if pd.isna(tags_str) or not tags_str:
        return []

    tags = [tag.strip() for tag in str(tags_str).split(',')]
    unique_tags = []
    seen_tags: Set[str] = set()
    for tag in tags:
        if tag and tag not in seen_tags:
            unique_tags.append(tag)
            seen_tags.add(tag)
    return unique_tags


def _parse_release_date_series(release_date: pd.Series) -> pd.Series:
    """
    将 `release_date` 序列稳健解析为 datetime。

    解析策略：
        1. 先用 `pd.to_datetime(..., errors='coerce')` 解析常见日期格式；
        2. 对解析失败但包含 4 位年份的值，回退为该年的 1 月 1 日；
        3. 对空串、None、TBD、Coming Soon 等占位值统一处理为缺失值。

    Args:
        release_date: 原始发布日期序列（字符串格式可能不统一）。

    Returns:
        与输入索引对齐的 datetime64[ns] 序列，无法解析的位置为 NaT。
    """
    cleaned = release_date.astype("string").str.strip()
    normalized = cleaned.str.lower()
    placeholders = {
        "",
        "nan",
        "none",
        "null",
        "tbd",
        "coming soon",
        "to be announced",
    }
    cleaned = cleaned.mask(normalized.isin(placeholders), pd.NA)

    parsed = pd.to_datetime(cleaned, errors='coerce')
    needs_year_fallback = parsed.isna() & cleaned.notna()
    if needs_year_fallback.any():
        year_only = cleaned[needs_year_fallback].str.extract(r"((?:19|20)\d{2})", expand=False)
        parsed.loc[needs_year_fallback] = pd.to_datetime(year_only, format="%Y", errors='coerce')

    return parsed


def _add_release_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    为数据添加可复用的发布时间字段。

    新增字段：
        - `release_datetime`: 解析后的发布时间（datetime）；
        - `release_year`: 发布年份（可空 Int64）。

    Args:
        df: 原始游戏数据。

    Returns:
        添加时间字段后的 DataFrame（不修改入参对象）。
    """
    result = df.copy()

    if 'release_date' not in result.columns:
        result['release_datetime'] = pd.NaT
        result['release_year'] = pd.Series(pd.NA, index=result.index, dtype='Int64')
        return result

    parsed_release_datetime = _parse_release_date_series(result['release_date'])
    result['release_datetime'] = parsed_release_datetime
    result['release_year'] = parsed_release_datetime.dt.year.astype('Int64')
    return result


def _get_high_positive_mask(
    df: pd.DataFrame,
    positive_rate_threshold: Optional[float] = None,
) -> pd.Series:
    """
    构造“高好评”布尔掩码。

    当 `positive_rate_threshold` 为空时，默认使用全局好评率中位数作为阈值。

    Args:
        df: 游戏数据，需包含 `positive_rate` 列。
        positive_rate_threshold: 可选阈值（百分比口径，例如 85 表示 85%）。

    Returns:
        与输入索引对齐的布尔序列，`True` 表示高好评游戏。
    """
    if df.empty:
        return pd.Series(index=df.index, dtype=bool)

    threshold = (
        float(df['positive_rate'].median())
        if positive_rate_threshold is None
        else float(positive_rate_threshold)
    )
    return df['positive_rate'] >= threshold


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


def _calculate_tag_probability_table(
    df: pd.DataFrame,
    positive_rate_threshold: Optional[float] = None,
) -> Tuple[pd.DataFrame, float, float]:
    """
    计算 Tag 维度的高好评条件概率基础表。

    Args:
        df: 游戏数据，需包含 `positive_rate`、`reviews`、`tags`。
        positive_rate_threshold: 高好评阈值；为空时使用全局中位数。

    Returns:
        三元组 `(tag_stats_df, threshold, p_high_positive_global)`：
            - `tag_stats_df` 包含 Tag 聚合统计与 Lift；
            - `threshold` 为本次计算采用的好评率阈值；
            - `p_high_positive_global` 为全局高好评概率。
    """
    columns = [
        'tag',
        'avg_positive_rate',
        'avg_reviews',
        'game_count',
        'high_positive_count',
        'p_high_positive_given_tag',
        'lift',
    ]
    if df.empty or 'tags' not in df.columns:
        return pd.DataFrame(columns=columns), 0.0, 0.0

    threshold = (
        float(df['positive_rate'].median())
        if positive_rate_threshold is None
        else float(positive_rate_threshold)
    )
    high_positive_mask = _get_high_positive_mask(df, threshold)
    p_high_positive_global = float(high_positive_mask.mean()) if not high_positive_mask.empty else 0.0

    exploded_df = (
        df[['positive_rate', 'reviews', 'tags']]
        .explode('tags')
        .rename(columns={'tags': 'tag'})
    )
    exploded_df = exploded_df[exploded_df['tag'].notna() & (exploded_df['tag'] != '')].copy()
    if exploded_df.empty:
        return pd.DataFrame(columns=columns), threshold, p_high_positive_global

    exploded_df['is_high_positive'] = high_positive_mask.reindex(exploded_df.index).astype(float).values
    tag_stats = exploded_df.groupby('tag', as_index=False).agg(
        avg_positive_rate=('positive_rate', 'median'),
        avg_reviews=('reviews', 'median'),
        game_count=('tag', 'size'),
        high_positive_count=('is_high_positive', 'sum'),
    )

    tag_stats['high_positive_count'] = tag_stats['high_positive_count'].astype(int)
    tag_stats['p_high_positive_given_tag'] = (
        tag_stats['high_positive_count'] / tag_stats['game_count']
    )
    if p_high_positive_global > 0:
        tag_stats['lift'] = tag_stats['p_high_positive_given_tag'] / p_high_positive_global
    else:
        tag_stats['lift'] = 0.0

    return tag_stats, threshold, p_high_positive_global


def calculate_global_stats(df: pd.DataFrame) -> Dict[str, float]:
    """
    计算全局统计数据（中位数阈值版）。

    说明：
        - 四象限阈值从“均值”切换为“中位数”，降低极端值对结果的影响；
        - 为兼容现有调用方，`avg_positive_rate` 与 `avg_reviews` 键名保留，
          但数值已改为中位数；
        - 同时返回 `mean_*` 字段，便于页面展示真实均值。

    Returns:
        包含阈值、均值、样本规模、全局高好评概率的字典。
    """
    if df.empty:
        return {
            'avg_positive_rate': 0.0,
            'avg_reviews': 0.0,
            'median_positive_rate': 0.0,
            'median_reviews': 0.0,
            'mean_positive_rate': 0.0,
            'mean_reviews': 0.0,
            'high_positive_ratio': 0.0,
            'total_games': 0.0,
        }

    median_positive_rate = float(df['positive_rate'].median())
    median_reviews = float(df['reviews'].median())
    high_positive_ratio = float((df['positive_rate'] >= median_positive_rate).mean())

    return {
        'avg_positive_rate': median_positive_rate,
        'avg_reviews': median_reviews,
        'median_positive_rate': median_positive_rate,
        'median_reviews': median_reviews,
        'mean_positive_rate': float(df['positive_rate'].mean()),
        'mean_reviews': float(df['reviews'].mean()),
        'high_positive_ratio': high_positive_ratio,
        'total_games': float(len(df)),
    }


def calculate_tag_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算每个 Tag 的统计数据，并附带 Lift 指标。

    该函数默认使用“全局好评率中位数”定义高好评游戏。

    Returns:
        DataFrame，包含以下字段：
            - `tag`: 标签名称；
            - `avg_positive_rate`: 标签下游戏平均好评率；
            - `avg_reviews`: 标签下游戏平均评论数；
            - `game_count`: 标签覆盖的游戏数量；
            - `high_positive_count`: 高好评游戏数量；
            - `p_high_positive_given_tag`: 条件概率 P(high_positive | Tag)；
            - `lift`: Tag Lift。
    """
    tag_stats, _, _ = _calculate_tag_probability_table(df)
    if tag_stats.empty:
        return tag_stats

    return tag_stats.sort_values(
        ['game_count', 'lift', 'avg_positive_rate'],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def calculate_tag_lift(
    df: pd.DataFrame,
    min_games: int = 5,
    positive_rate_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    计算单个 Tag 的 Lift：`P(high_positive | Tag) / P(high_positive_global)`。

    Args:
        df: 游戏数据。
        min_games: 最小样本量，小于该值的 Tag 会被过滤。
        positive_rate_threshold: 高好评阈值；为空时使用全局中位数。

    Returns:
        DataFrame，按 Lift 降序排列，字段包括概率、Lift 与阈值信息。
    """
    tag_stats, threshold, p_high_positive_global = _calculate_tag_probability_table(
        df,
        positive_rate_threshold=positive_rate_threshold,
    )
    columns = [
        'tag',
        'game_count',
        'high_positive_count',
        'p_high_positive_given_tag',
        'p_high_positive_global',
        'lift',
        'positive_rate_threshold',
    ]
    if tag_stats.empty:
        return pd.DataFrame(columns=columns)

    result = tag_stats[tag_stats['game_count'] >= min_games].copy()
    if result.empty:
        return pd.DataFrame(columns=columns)

    result['p_high_positive_global'] = p_high_positive_global
    result['positive_rate_threshold'] = threshold
    return result[columns].sort_values(
        ['lift', 'game_count'],
        ascending=[False, False],
    ).reset_index(drop=True)


def calculate_tag_combo_synergy(
    df: pd.DataFrame,
    min_combo_games: int = 5,
    top_n_tags: Optional[int] = None,
    positive_rate_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    计算 Tag 两两组合的 Lift 与协同效应（Synergy）。

    公式：
        - `pair_lift = P(high_positive | TagA, TagB) / P(high_positive_global)`
        - `expected_pair_lift = lift(TagA) * lift(TagB)`
        - `synergy_ratio = pair_lift / expected_pair_lift`
        - `synergy_delta = pair_lift - expected_pair_lift`

    Args:
        df: 游戏数据。
        min_combo_games: 组合最小样本量。
        top_n_tags: 仅使用游戏数最高的前 N 个 Tag 进行组合计算，None 表示不限制。
        positive_rate_threshold: 高好评阈值；为空时使用全局中位数。

    Returns:
        DataFrame，包含组合 Lift、个体 Lift 与协同指标。
    """
    columns = ['tag1','tag2','tag_pair','game_count','high_positive_count','p_high_positive_given_combo','p_high_positive_global','pair_lift','lift_tag_a','lift_tag_b','expected_pair_lift','synergy_score','synergy_delta','positive_rate_threshold','tag_a','tag_b','synergy_ratio']
    if df.empty or 'tags' not in df.columns:
        return pd.DataFrame(columns=columns)

    threshold = (
        float(df['positive_rate'].median())
        if positive_rate_threshold is None
        else float(positive_rate_threshold)
    )
    high_positive_mask = _get_high_positive_mask(df, threshold)
    p_high_positive_global = float(high_positive_mask.mean()) if not high_positive_mask.empty else 0.0
    if p_high_positive_global == 0.0:
        return pd.DataFrame(columns=columns)

    tag_lift_df = calculate_tag_lift(
        df,
        min_games=1,
        positive_rate_threshold=threshold,
    )
    if tag_lift_df.empty:
        return pd.DataFrame(columns=columns)

    if top_n_tags is not None and top_n_tags > 0:
        candidate_tags = set(
            tag_lift_df.sort_values('game_count', ascending=False)
            .head(top_n_tags)['tag']
            .tolist()
        )
    else:
        candidate_tags = set(tag_lift_df['tag'].tolist())
    if not candidate_tags:
        return pd.DataFrame(columns=columns)

    lift_map = tag_lift_df.set_index('tag')['lift'].to_dict()
    pair_records = []
    for row_pos, tags in enumerate(df['tags']):
        if not isinstance(tags, list) or len(tags) < 2:
            continue

        filtered_tags = sorted({tag for tag in tags if tag in candidate_tags})
        if len(filtered_tags) < 2:
            continue

        is_high_positive = int(high_positive_mask.iloc[row_pos])
        for tag_a, tag_b in combinations(filtered_tags, 2):
            pair_records.append((tag_a, tag_b, is_high_positive))

    if not pair_records:
        return pd.DataFrame(columns=columns)

    pair_df = pd.DataFrame(pair_records, columns=['tag_a', 'tag_b', 'is_high_positive'])
    combo_stats = pair_df.groupby(['tag_a', 'tag_b'], as_index=False).agg(
        game_count=('is_high_positive', 'size'),
        high_positive_count=('is_high_positive', 'sum'),
    )
    combo_stats = combo_stats[combo_stats['game_count'] >= min_combo_games].copy()
    if combo_stats.empty:
        return pd.DataFrame(columns=columns)

    combo_stats['p_high_positive_given_combo'] = (
        combo_stats['high_positive_count'] / combo_stats['game_count']
    )
    combo_stats['p_high_positive_global'] = p_high_positive_global
    combo_stats['pair_lift'] = combo_stats['p_high_positive_given_combo'] / p_high_positive_global
    combo_stats['lift_tag_a'] = combo_stats['tag_a'].map(lift_map)
    combo_stats['lift_tag_b'] = combo_stats['tag_b'].map(lift_map)
    combo_stats['expected_pair_lift'] = combo_stats['lift_tag_a'] * combo_stats['lift_tag_b']
    combo_stats['synergy_ratio'] = combo_stats['pair_lift'] / combo_stats['expected_pair_lift'].replace(0, pd.NA)
    combo_stats['synergy_ratio'] = combo_stats['synergy_ratio'].fillna(0.0)
    combo_stats['synergy_delta'] = combo_stats['pair_lift'] - combo_stats['expected_pair_lift']
    combo_stats['tag1'] = combo_stats['tag_a']
    combo_stats['tag2'] = combo_stats['tag_b']
    combo_stats['synergy_score'] = combo_stats['synergy_ratio']
    combo_stats['tag_pair'] = combo_stats['tag1'] + ' + ' + combo_stats['tag2']
    combo_stats['positive_rate_threshold'] = threshold

    return combo_stats[columns].sort_values(['synergy_score', 'pair_lift', 'game_count'], ascending=[False, False, False]).reset_index(drop=True)


def calculate_yearly_trends(
    df: pd.DataFrame,
    min_games_per_year: int = 1,
) -> pd.DataFrame:
    """
    基于 `release_date` 计算年度趋势（平均好评率 + 游戏数量）。

    该函数会自动进行日期解析：
        - 若输入数据尚未包含 `release_year`，会先进行稳健日期解析；
        - 对无法解析的日期自动忽略，避免污染趋势结果。

    Args:
        df: 游戏数据。
        min_games_per_year: 每年最小样本量过滤阈值。

    Returns:
        DataFrame，字段为 `year`、`avg_positive_rate`、`game_count`。
    """
    columns = ['year', 'avg_positive_rate', 'game_count']
    if df.empty:
        return pd.DataFrame(columns=columns)

    working_df = df
    if 'release_year' not in working_df.columns or 'release_datetime' not in working_df.columns:
        working_df = _add_release_time_features(df)

    valid_df = working_df.dropna(subset=['release_year'])
    if valid_df.empty:
        return pd.DataFrame(columns=columns)

    yearly_trend = (
        valid_df.groupby('release_year', as_index=False)
        .agg(
            avg_positive_rate=('positive_rate', 'median'),
            game_count=('positive_rate', 'size'),
        )
        .rename(columns={'release_year': 'year'})
        .sort_values('year')
    )
    yearly_trend['year'] = yearly_trend['year'].astype(int)

    if min_games_per_year > 1:
        yearly_trend = yearly_trend[yearly_trend['game_count'] >= min_games_per_year]

    return yearly_trend.reset_index(drop=True)


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


def calculate_combo_verdict(df: pd.DataFrame, combo_tags: List[str]) -> Dict[str, object]:
    """
    计算 Tag 组合的 5 档分项评估。

    评分维度：
        - Q (Quality): 组合中位数好评率在同阶组合中的百分位；
        - H (Heat): 组合中位数评论数（log1p）在同阶组合中的百分位；
        - S (Synergy): 相对全局 Lift 与相对最大子组合 Lift 的综合分；
        - M (Momentum): 最近 3 年供给与质量趋势分；
        - C (Confidence): 样本量置信度分。

    Args:
        df: 游戏数据，需包含 `positive_rate`、`reviews`、`tags` 或 `tags_str`。
        combo_tags: 目标组合的 Tag 列表。

    Returns:
        Dict，包含分项分数与五维 `tier_evaluations`（Q/H/S/M/C）。
    """
    normalized_tags: List[str] = []
    for tag in combo_tags or []:
        cleaned_tag = str(tag).strip()
        if cleaned_tag and cleaned_tag not in normalized_tags:
            normalized_tags.append(cleaned_tag)

    def _percentile_to_tier(percentile_value: float) -> str:
        if pd.isna(percentile_value):
            return "差"

        value = float(percentile_value)
        if value >= 90:
            return "出色"
        if value >= 70:
            return "较好"
        if value >= 40:
            return "一般"
        if value >= 15:
            return "较差"
        return "差"

    def _synergy_to_tier(lift_value: float) -> str:
        if pd.isna(lift_value):
            return "差"

        value = float(lift_value)
        if value > 1.25:
            return "出色"
        if value >= 1.05:
            return "较好"
        if value >= 0.95:
            return "一般"
        if value >= 0.8:
            return "较差"
        return "差"

    def _confidence_to_tier(sample_size: int) -> str:
        if sample_size >= 200:
            return "出色"
        if sample_size >= 80:
            return "较好"
        if sample_size >= 30:
            return "一般"
        if sample_size >= 10:
            return "较差"
        return "差"

    def _momentum_to_tier(supply_growth: float, quality_delta: float) -> str:
        if supply_growth > 0.10:
            supply_trend = "up"
        elif supply_growth < -0.10:
            supply_trend = "down"
        else:
            supply_trend = "flat"

        if quality_delta > 1.0:
            quality_trend = "up"
        elif quality_delta < -1.0:
            quality_trend = "down"
        else:
            quality_trend = "flat"

        if supply_trend == "up" and quality_trend == "up":
            return "出色"
        if (
            (supply_trend == "flat" and quality_trend == "up")
            or (supply_trend == "up" and quality_trend == "flat")
        ):
            return "较好"
        if (
            (supply_trend == "down" and quality_trend == "up")
            or (supply_trend == "flat" and quality_trend == "flat")
        ):
            return "一般"
        if (
            (supply_trend == "up" and quality_trend == "down")
            or (supply_trend == "flat" and quality_trend == "down")
            or (supply_trend == "down" and quality_trend == "flat")
        ):
            return "较差"
        return "差"

    def _format_tier_summary(tier_evaluations: Dict[str, str]) -> str:
        ordered_keys = ['Q', 'H', 'S', 'M', 'C']
        return ' / '.join([f"{key}:{tier_evaluations.get(key, '差')}" for key in ordered_keys])

    def _lift_to_score(lift_value: float) -> float:
        if pd.isna(lift_value):
            return 0.0
        return float(max(0.0, min(100.0, float(lift_value) * 50.0)))

    def _confidence_score(sample_size: int) -> float:
        if sample_size >= 200:
            return 100.0
        if sample_size >= 80:
            return 70.0
        if sample_size >= 30:
            return 40.0
        return 10.0

    def _build_empty_result(reason: str) -> Dict[str, object]:
        score_dict = {
            'Q': 0.0,
            'H': 0.0,
            'S': 0.0,
            'M': 0.0,
            'C': 10.0,
        }
        score_dict['total'] = 0.0
        tier_evaluations = {
            'Q': _percentile_to_tier(score_dict['Q']),
            'H': _percentile_to_tier(score_dict['H']),
            'S': _synergy_to_tier(0.0),
            'M': "差",
            'C': _confidence_to_tier(0),
        }
        return {
            'combo_tags': normalized_tags,
            'combo_key': ' + '.join(normalized_tags),
            'sample_size': 0,
            'scores': score_dict,
            'tier_evaluations': tier_evaluations,
            'verdict': _format_tier_summary(tier_evaluations),
            'details': {
                'reason': reason,
                'quality_percentile': 0.0,
                'heat_percentile': 0.0,
                'lift_vs_global': 0.0,
                'lift_vs_max_sub_combo': 0.0,
                'momentum_years': [],
                'supply_score': 0.0,
                'quality_score': 0.0,
                'supply_growth': 0.0,
                'quality_delta': 0.0,
            },
        }

    if not normalized_tags:
        return _build_empty_result("empty_combo_tags")

    if df is None or df.empty:
        return _build_empty_result("empty_data")

    required_columns = {'positive_rate', 'reviews'}
    if not required_columns.issubset(df.columns):
        return _build_empty_result("missing_required_columns")

    working_df = df.copy()
    working_df['positive_rate'] = pd.to_numeric(working_df['positive_rate'], errors='coerce')
    working_df['reviews'] = pd.to_numeric(working_df['reviews'], errors='coerce')
    working_df = working_df.dropna(subset=['positive_rate', 'reviews'])
    if working_df.empty:
        return _build_empty_result("no_valid_metric_rows")

    if 'tags' not in working_df.columns:
        if 'tags_str' in working_df.columns:
            working_df['tags'] = working_df['tags_str'].apply(parse_tags)
        else:
            return _build_empty_result("missing_tags_columns")
    else:
        working_df['tags'] = working_df['tags'].apply(
            lambda value: value
            if isinstance(value, list)
            else parse_tags(value)
        )

    combo_size = len(normalized_tags)
    target_combo_key = tuple(sorted(normalized_tags))
    combo_df = get_games_by_tags(working_df, normalized_tags)
    sample_size = int(len(combo_df))

    combo_median_positive_rate = float(combo_df['positive_rate'].median()) if sample_size > 0 else 0.0
    combo_median_reviews = float(combo_df['reviews'].median()) if sample_size > 0 else 0.0

    combo_rows: List[Dict[str, object]] = []
    for _, row in working_df[['tags', 'positive_rate', 'reviews']].iterrows():
        row_tags = row['tags']
        if not isinstance(row_tags, list):
            continue
        unique_tags = sorted({str(tag).strip() for tag in row_tags if str(tag).strip()})
        if len(unique_tags) < combo_size:
            continue

        for combo in combinations(unique_tags, combo_size):
            combo_rows.append({
                'combo_key': combo,
                'positive_rate': float(row['positive_rate']),
                'reviews': float(row['reviews']),
            })

    if combo_rows:
        all_combo_df = pd.DataFrame(combo_rows)
        all_combo_stats = all_combo_df.groupby('combo_key', as_index=False).agg(
            median_positive_rate=('positive_rate', 'median'),
            median_reviews=('reviews', 'median'),
            combo_game_count=('combo_key', 'size'),
        )
    else:
        all_combo_stats = pd.DataFrame(columns=['combo_key', 'median_positive_rate', 'median_reviews', 'combo_game_count'])

    if sample_size > 0 and not all_combo_stats.empty:
        quality_series = all_combo_stats['median_positive_rate'].dropna()
        if quality_series.empty:
            quality_score = 0.0
        else:
            quality_score = float((quality_series <= combo_median_positive_rate).mean() * 100)

        log_reviews_series = all_combo_stats['median_reviews'].dropna().clip(lower=0).apply(math.log1p)
        combo_log_reviews = math.log1p(max(0.0, combo_median_reviews))
        if log_reviews_series.empty:
            heat_score = 0.0
        else:
            heat_score = float((log_reviews_series <= combo_log_reviews).mean() * 100)
    else:
        quality_score = 0.0
        heat_score = 0.0

    high_positive_threshold = float(working_df['positive_rate'].median())
    p_high_positive_global = float((working_df['positive_rate'] >= high_positive_threshold).mean())
    if sample_size > 0 and p_high_positive_global > 0:
        p_high_positive_combo = float((combo_df['positive_rate'] >= high_positive_threshold).mean())
        lift_vs_global = p_high_positive_combo / p_high_positive_global
    else:
        lift_vs_global = 0.0

    max_sub_combo_lift = 0.0
    if combo_size > 1 and p_high_positive_global > 0:
        for sub_combo_size in range(1, combo_size):
            for sub_combo in combinations(normalized_tags, sub_combo_size):
                sub_combo_df = get_games_by_tags(working_df, list(sub_combo))
                if sub_combo_df.empty:
                    continue

                p_high_positive_sub_combo = float((sub_combo_df['positive_rate'] >= high_positive_threshold).mean())
                sub_combo_lift = p_high_positive_sub_combo / p_high_positive_global
                max_sub_combo_lift = max(max_sub_combo_lift, sub_combo_lift)

    if max_sub_combo_lift > 0:
        lift_vs_max_sub_combo = lift_vs_global / max_sub_combo_lift
    elif lift_vs_global > 0:
        lift_vs_max_sub_combo = 1.0
    else:
        lift_vs_max_sub_combo = 0.0

    synergy_score = (
        0.6 * _lift_to_score(lift_vs_global)
        + 0.4 * _lift_to_score(lift_vs_max_sub_combo)
    )

    if 'release_year' not in working_df.columns or 'release_datetime' not in working_df.columns:
        working_df = _add_release_time_features(working_df)
    if 'release_year' not in combo_df.columns:
        combo_df = _add_release_time_features(combo_df)

    valid_global_years = working_df['release_year'].dropna()
    if sample_size > 0 and not valid_global_years.empty:
        latest_year = int(valid_global_years.max())
        momentum_years = [latest_year - 2, latest_year - 1, latest_year]

        combo_yearly = (
            combo_df.dropna(subset=['release_year'])
            .groupby('release_year', as_index=False)
            .agg(
                game_count=('positive_rate', 'size'),
                avg_positive_rate=('positive_rate', 'median'),
            )
        )
        combo_yearly['release_year'] = combo_yearly['release_year'].astype(int)
        combo_yearly = combo_yearly.set_index('release_year').reindex(momentum_years)

        supply_series = combo_yearly['game_count'].fillna(0.0).astype(float)
        global_median_quality = float(working_df['positive_rate'].median())
        quality_series = combo_yearly['avg_positive_rate'].fillna(global_median_quality).astype(float)

        supply_start = float(supply_series.iloc[0])
        supply_end = float(supply_series.iloc[-1])
        if supply_start <= 0 and supply_end > 0:
            supply_growth = 1.0
        elif supply_start <= 0 and supply_end <= 0:
            supply_growth = 0.0
        else:
            supply_growth = (supply_end - supply_start) / max(1.0, supply_start)
        supply_growth = float(max(-1.0, min(1.0, supply_growth)))

        quality_delta = float(quality_series.iloc[-1] - quality_series.iloc[0])
        quality_delta = float(max(-15.0, min(15.0, quality_delta)))

        supply_score = 50.0 + 50.0 * supply_growth
        quality_trend_score = 50.0 + (quality_delta / 15.0) * 50.0
        momentum_score = 0.5 * supply_score + 0.5 * quality_trend_score
    else:
        momentum_years = []
        supply_growth = 0.0
        quality_delta = 0.0
        supply_score = 50.0
        quality_trend_score = 50.0
        momentum_score = 50.0

    confidence_score = _confidence_score(sample_size)

    scores = {
        'Q': round(float(quality_score), 2),
        'H': round(float(heat_score), 2),
        'S': round(float(synergy_score), 2),
        'M': round(float(momentum_score), 2),
        'C': round(float(confidence_score), 2),
    }
    total_score = (
        0.25 * scores['Q']
        + 0.20 * scores['H']
        + 0.25 * scores['S']
        + 0.15 * scores['M']
        + 0.15 * scores['C']
    )
    scores['total'] = round(float(total_score), 2)

    tier_evaluations = {
        'Q': _percentile_to_tier(scores['Q']),
        'H': _percentile_to_tier(scores['H']),
        'S': _synergy_to_tier(lift_vs_max_sub_combo),
        'M': _momentum_to_tier(supply_growth, quality_delta),
        'C': _confidence_to_tier(sample_size),
    }

    if sample_size > 0 and target_combo_key in set(all_combo_stats['combo_key']):
        combo_rank = int(
            all_combo_stats['median_positive_rate']
            .rank(method='min', ascending=False)
            .loc[all_combo_stats['combo_key'] == target_combo_key]
            .iloc[0]
        )
        total_combos = int(len(all_combo_stats))
    else:
        combo_rank = 0
        total_combos = int(len(all_combo_stats))

    return {
        'combo_tags': normalized_tags,
        'combo_key': ' + '.join(normalized_tags),
        'sample_size': sample_size,
        'scores': scores,
        'tier_evaluations': tier_evaluations,
        'verdict': _format_tier_summary(tier_evaluations),
        'details': {
            'quality_percentile': scores['Q'],
            'heat_percentile': scores['H'],
            'combo_median_positive_rate': round(combo_median_positive_rate, 4),
            'combo_median_reviews': round(combo_median_reviews, 2),
            'lift_vs_global': round(float(lift_vs_global), 4),
            'lift_vs_max_sub_combo': round(float(lift_vs_max_sub_combo), 4),
            'high_positive_threshold': round(high_positive_threshold, 4),
            'momentum_years': momentum_years,
            'supply_score': round(float(supply_score), 2),
            'quality_score': round(float(quality_trend_score), 2),
            'supply_growth': round(float(supply_growth), 4),
            'quality_delta': round(float(quality_delta), 4),
            'combo_rank': combo_rank,
            'total_combos': total_combos,
        },
    }


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
