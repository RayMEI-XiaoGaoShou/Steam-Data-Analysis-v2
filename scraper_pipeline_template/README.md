# Steam 游戏数据高可用自动化爬虫流水线 (Scraper Pipeline)

这个文件夹提取了本次项目中**最精华、可复用的数据获取与清洗代码**。它包含了一套完整支持**断点续传、多分片并行、防反爬、混合数据源**的生产级 Steam 爬虫系统。

您可以直接保留此文件夹。在未来（例如每 6 个月），当您需要再次获取 Steam 最新数据时，可以开箱即用这套流水线。

## 💡 核心设计与优势

1. **混合数据获取方案**：
   - **Seed 阶段 (API)**：使用极高速的官方 `appreviews` 接口全量扫街，获取 16.5w 游戏的 `review_count` 和 `positive_rate`。
   - **Details 阶段 (HTML)**：仅针对初筛达标（如评论数 > 200）的几千款游戏，缓慢请求 Steam 商店网页获取 `release_date` 和 `Tags`，从而绕过大量垃圾游戏的请求耗时。

2. **高可用与断点续传**：
   - 所有进度均实时写入 SQLite 数据库（`steam_data_cache*.db`）。
   - 内置 `scanned_apps` 表和 `last_updated` 时间戳机制。无论断网、关机、IP被封，再次启动脚本时**只会从失败处继续**，绝不重复抓取。

3. **多分片并行架构 (Hash Sharding)**：
   - 采用 `hash(appid) % N` 算法将全量任务打散为互不重叠的并行队列。
   - 可以同时拉起 3~5 个后台进程一起跑，让原来需要几天的工作压缩至十几个小时。

---

## 🛠️ 文件清单与功能说明

### 1. 核心爬虫引擎
*   **`steam_scraper_advanced.py`**
    *   **作用**：全套流程的“发动机”。内置了拉取全量 AppID、调用 API 获取评论数、请求网页抓取 Tags 的完整能力。
    *   **参数**：支持通过 `--phase seed|details` 切换模式，通过 `--shard-index`, `--shard-count` 切换分片。

### 2. 第一阶段：Seed（海选初筛）
*   **`start_seed_5_shards.sh`**
    *   **作用**：一键启动 5 个并发的 Seed 进程，对 Steam 的 16.5 万库进行地毯式排查。
*   **`check_seed_5_shards.py`**
    *   **作用**：看板脚本。随时运行以查看那 5 个 Seed 进程的扫描进度、总完成度、入库的高热度游戏数量。

### 3. 第二阶段：Details（深度抓取）
*   **`prepare_details_parallel_from_current.py`**
    *   **作用**：在 Seed 跑完（或跑了一部分）后，把需要抓页面信息的优质游戏提取出来，切分成 3 个等份的并行子数据库，防止写库锁冲突。
*   **`start_details_parallel3.sh`**
    *   **作用**：一键启动 3 个并发的 Details 进程，开始缓慢而稳定地爬取这批游戏的 Tags 和发售日。
*   **`check_details_parallel3_progress.py`**
    *   **作用**：Details 阶段的看板脚本，随时查看三个进程的完成率。

### 4. 第三阶段：合并与导出
*   **`merge_details_into_hash_shards.py`**
    *   **作用**：Details 跑完后，把分散的子数据库结果，按 `appid` 精准回写合并到主库中。
*   **`build_final_dataset.py`**
    *   **作用**：最后一脚。把所有散落的分片合并成一个唯一的 `steam_data_final_merged.db`，并直接导出为前端可以直接使用的高质量 `steam_data_final_merged.csv`。

---

## 🚀 未来复用流程（定期更新指南）

假设 6 个月后你需要获取最新数据，只需按顺序执行：

1. **(准备)** 确保依赖齐全 (`pip install -r requirements.txt`)。
2. **(执行 Seed)** 运行 `bash start_seed_5_shards.sh`。让电脑挂机半天，偶尔用 `python check_seed_5_shards.py` 看进度。
3. **(准备 Details)** 运行 `python prepare_details_parallel_from_current.py`，把需要抓页面的游戏筛出来。
4. **(执行 Details)** 运行 `bash start_details_parallel3.sh`。让它继续挂机跑，并用 `python check_details_parallel3_progress.py` 看进度。
5. **(合并结果)** 运行 `python merge_details_into_hash_shards.py`。
6. **(导出数据)** 运行 `python build_final_dataset.py`。拿到新鲜出炉的 CSV 文件。
7. **(上传更新)** 把新的 CSV 导入 Supabase（如有配置），你的决策系统瞬间更新为当前时刻的最新市场状况！