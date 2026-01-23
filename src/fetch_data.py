"""
Steam 游戏数据获取脚本
从 SteamSpy API 和 Steam Store API 获取2025年发布的游戏数据
"""

import requests
import json
import time
import os
from datetime import datetime
from pathlib import Path

# 配置
DATA_DIR = Path(__file__).parent.parent / "data"
CACHE_FILE = DATA_DIR / "games_cache.json"
OUTPUT_FILE = DATA_DIR / "steam_games_2025.csv"

# API 配置
STEAMSPY_BASE = "https://steamspy.com/api.php"
STEAM_STORE_BASE = "https://store.steampowered.com/api/appdetails"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# 请求间隔（秒）
STEAMSPY_DELAY = 1.0  # SteamSpy 要求 1 次/秒
STEAM_STORE_DELAY = 0.3  # Steam Store API 稍宽松


def ensure_data_dir():
    """确保数据目录存在"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_cache():
    """加载缓存数据"""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"games": {}, "last_update": None}


def save_cache(cache):
    """保存缓存数据"""
    cache["last_update"] = datetime.now().isoformat()
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_all_game_ids():
    """
    从 SteamSpy 获取所有游戏 ID 列表
    注意：/all 端点每页返回 1000 个游戏，限制 1次/60秒
    """
    print("正在获取游戏 ID 列表...")
    all_games = {}
    page = 0
    
    while True:
        try:
            url = f"{STEAMSPY_BASE}?request=all&page={page}"
            print(f"  获取第 {page + 1} 页...")
            
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                print(f"  第 {page + 1} 页为空，获取完成")
                break
            
            all_games.update(data)
            print(f"  已获取 {len(all_games)} 个游戏")
            
            page += 1
            
            # SteamSpy /all 端点限制：1次/60秒
            print(f"  等待 60 秒...")
            time.sleep(60)
            
        except requests.exceptions.RequestException as e:
            print(f"  请求失败: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"  JSON 解析失败: {e}")
            break
    
    return all_games


def fetch_game_release_date(appid):
    """
    从 Steam Store API 获取游戏发布日期
    返回: (release_date_str, is_2025) 或 (None, False)
    """
    try:
        url = f"{STEAM_STORE_BASE}?appids={appid}&cc=us&l=en"
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if str(appid) not in data:
            return None, False
        
        app_data = data[str(appid)]
        
        if not app_data.get("success"):
            return None, False
        
        release_info = app_data.get("data", {}).get("release_date", {})
        
        if release_info.get("coming_soon"):
            return None, False
        
        date_str = release_info.get("date", "")
        
        # 检查是否为 2025 年
        # Steam 日期格式通常是 "Jan 15, 2025" 或 "15 Jan, 2025"
        is_2025 = "2025" in date_str
        
        return date_str, is_2025
        
    except Exception as e:
        return None, False


def fetch_game_details(appid):
    """
    从 SteamSpy 获取游戏详情（评论数、好评率、Tags）
    """
    try:
        url = f"{STEAMSPY_BASE}?request=appdetails&appid={appid}"
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if not data or data.get("appid") == 999999:
            return None
        
        # 计算好评率
        positive = data.get("positive", 0)
        negative = data.get("negative", 0)
        total_reviews = positive + negative
        
        if total_reviews == 0:
            positive_rate = 0
        else:
            positive_rate = round(positive / total_reviews * 100, 2)
        
        # 获取 Tags（按权重排序，取前5个）
        tags = data.get("tags", {})
        if isinstance(tags, dict):
            sorted_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)
            top_tags = [tag[0] for tag in sorted_tags[:5]]
        else:
            top_tags = []
        
        return {
            "appid": appid,
            "name": data.get("name", ""),
            "positive": positive,
            "negative": negative,
            "total_reviews": total_reviews,
            "positive_rate": positive_rate,
            "tags": top_tags,
            "owners": data.get("owners", ""),
            "average_playtime": data.get("average_forever", 0),
        }
        
    except Exception as e:
        print(f"    获取 {appid} 详情失败: {e}")
        return None


def fetch_2025_games(min_reviews=200, max_games=None):
    """
    主函数：获取2025年发布的游戏数据
    
    Args:
        min_reviews: 最小评论数阈值
        max_games: 最大获取游戏数（用于测试）
    """
    ensure_data_dir()
    cache = load_cache()
    
    # Step 1: 获取所有游戏 ID
    print("\n=== Step 1: 获取游戏 ID 列表 ===")
    
    if "all_games" in cache and cache["all_games"]:
        print("使用缓存的游戏 ID 列表")
        all_games = cache["all_games"]
    else:
        all_games = fetch_all_game_ids()
        cache["all_games"] = all_games
        save_cache(cache)
    
    print(f"共有 {len(all_games)} 个游戏")
    
    # Step 2: 筛选可能符合条件的游戏（基于 SteamSpy 的初步数据）
    print("\n=== Step 2: 初步筛选（评论数 >= {min_reviews}）===")
    
    candidates = []
    for appid, info in all_games.items():
        # SteamSpy /all 返回的数据包含 positive 和 negative
        positive = info.get("positive", 0) or 0
        negative = info.get("negative", 0) or 0
        total = positive + negative
        
        if total >= min_reviews:
            candidates.append({
                "appid": int(appid),
                "name": info.get("name", ""),
                "total_reviews": total
            })
    
    print(f"评论数 >= {min_reviews} 的游戏: {len(candidates)} 个")
    
    # 按评论数排序
    candidates.sort(key=lambda x: x["total_reviews"], reverse=True)
    
    if max_games:
        candidates = candidates[:max_games]
        print(f"测试模式：只处理前 {max_games} 个游戏")
    
    # Step 3: 获取发布日期，筛选2025年游戏
    print("\n=== Step 3: 筛选2025年发布的游戏 ===")
    
    games_2025 = []
    processed = 0
    
    for candidate in candidates:
        appid = candidate["appid"]
        processed += 1
        
        # 检查缓存
        cache_key = str(appid)
        if cache_key in cache["games"]:
            cached_game = cache["games"][cache_key]
            if cached_game.get("is_2025"):
                games_2025.append(cached_game)
            continue
        
        print(f"  [{processed}/{len(candidates)}] 检查 {candidate['name'][:30]}...", end=" ")
        
        # 获取发布日期
        release_date, is_2025 = fetch_game_release_date(appid)
        time.sleep(STEAM_STORE_DELAY)
        
        if not is_2025:
            print("非2025年")
            cache["games"][cache_key] = {"appid": appid, "is_2025": False}
            continue
        
        print(f"2025年! 获取详情...", end=" ")
        
        # 获取详情
        details = fetch_game_details(appid)
        time.sleep(STEAMSPY_DELAY)
        
        if details:
            details["release_date"] = release_date
            details["is_2025"] = True
            games_2025.append(details)
            cache["games"][cache_key] = details
            print(f"✓ 好评率 {details['positive_rate']}%")
        else:
            print("详情获取失败")
            cache["games"][cache_key] = {"appid": appid, "is_2025": False}
        
        # 定期保存缓存
        if processed % 50 == 0:
            save_cache(cache)
            print(f"  --- 已保存缓存，当前找到 {len(games_2025)} 个2025年游戏 ---")
    
    # 最终保存
    save_cache(cache)
    
    print(f"\n=== 完成 ===")
    print(f"2025年发布且评论数 >= {min_reviews} 的游戏: {len(games_2025)} 个")
    
    return games_2025


def save_to_csv(games):
    """将游戏数据保存为 CSV"""
    import pandas as pd
    
    # 展开 tags 列
    rows = []
    for game in games:
        row = {
            "appid": game["appid"],
            "name": game["name"],
            "release_date": game.get("release_date", ""),
            "total_reviews": game["total_reviews"],
            "positive": game["positive"],
            "negative": game["negative"],
            "positive_rate": game["positive_rate"],
            "owners": game.get("owners", ""),
            "average_playtime": game.get("average_playtime", 0),
        }
        
        # 添加 Tag 列
        tags = game.get("tags", [])
        for i in range(5):
            row[f"tag_{i+1}"] = tags[i] if i < len(tags) else ""
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"数据已保存到: {OUTPUT_FILE}")
    
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("Steam 2025 游戏数据获取脚本")
    print("=" * 60)
    
    # 获取数据（测试模式：只处理前100个高评论游戏）
    # 正式运行时去掉 max_games 参数
    games = fetch_2025_games(min_reviews=200, max_games=100)
    
    if games:
        df = save_to_csv(games)
        print("\n数据预览:")
        print(df.head(10).to_string())
