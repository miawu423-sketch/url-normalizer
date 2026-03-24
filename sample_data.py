#!/usr/bin/env python3
"""
数据抽样脚本：
任务①：在 engine_type=doubao 的数据中随机抽 100 个 prompt 及其对应的全部数据行
任务②：在 engine_type=google 的数据中随机抽 100 个 prompt，每个 prompt 随机抽一个 query，
       把这些 query 对应的所有数据行抽出来
"""

import pandas as pd
import random

# 设置随机种子以保证可复现性（可选）
random.seed(42)

# 读取原始数据
input_file = '/Users/miawu/Downloads/英文-20260321网页库、索引库覆盖率-人评抽随机.csv'
df = pd.read_csv(input_file)

print(f"原始数据共 {len(df)} 行")
print(f"列名: {list(df.columns)}")

# ===========================================
# 任务①：doubao 数据 - 随机抽 100 个 prompt 及其全部数据行
# ===========================================
doubao_df = df[df['engine_type'] == 'doubao']
print(f"\nDoubao 数据共 {len(doubao_df)} 行")

# 获取所有唯一的 prompt
doubao_prompts = doubao_df['prompt'].unique().tolist()
print(f"Doubao 唯一 prompt 数量: {len(doubao_prompts)}")

# 随机抽取 100 个 prompt（如果不足 100 个则全部取）
sample_count_doubao = min(100, len(doubao_prompts))
sampled_doubao_prompts = random.sample(doubao_prompts, sample_count_doubao)
print(f"抽取了 {sample_count_doubao} 个 prompt")

# 获取这些 prompt 对应的所有数据行
doubao_result = doubao_df[doubao_df['prompt'].isin(sampled_doubao_prompts)]
print(f"任务①结果：{len(doubao_result)} 行数据")

# 保存任务①结果
output_file_1 = '/Users/miawu/Downloads/任务1_doubao_100prompts抽样.csv'
doubao_result.to_csv(output_file_1, index=False, encoding='utf-8-sig')
print(f"任务①结果已保存到: {output_file_1}")

# ===========================================
# 任务②：google 数据 - 随机抽 100 个 prompt，每个随机抽一个 query
# ===========================================
google_df = df[df['engine_type'] == 'google']
print(f"\nGoogle 数据共 {len(google_df)} 行")

# 获取所有唯一的 prompt
google_prompts = google_df['prompt'].unique().tolist()
print(f"Google 唯一 prompt 数量: {len(google_prompts)}")

# 随机抽取 100 个 prompt（如果不足 100 个则全部取）
sample_count_google = min(100, len(google_prompts))
sampled_google_prompts = random.sample(google_prompts, sample_count_google)
print(f"抽取了 {sample_count_google} 个 prompt")

# 对每个抽中的 prompt，随机抽一个 query
sampled_queries = []
for prompt in sampled_google_prompts:
    # 获取该 prompt 下的所有 query
    prompt_queries = google_df[google_df['prompt'] == prompt]['query'].unique().tolist()
    # 随机选一个 query
    selected_query = random.choice(prompt_queries)
    sampled_queries.append((prompt, selected_query))

print(f"抽取了 {len(sampled_queries)} 个 (prompt, query) 组合")

# 获取这些 query 对应的所有数据行
# 注意：同一个 query 可能对应多行数据（不同的 url/rank 等）
google_result_list = []
for prompt, query in sampled_queries:
    # 筛选 prompt 和 query 都匹配的行
    matching_rows = google_df[(google_df['prompt'] == prompt) & (google_df['query'] == query)]
    google_result_list.append(matching_rows)

google_result = pd.concat(google_result_list, ignore_index=True)
print(f"任务②结果：{len(google_result)} 行数据")

# 保存任务②结果
output_file_2 = '/Users/miawu/Downloads/任务2_google_100prompts_随机query抽样.csv'
google_result.to_csv(output_file_2, index=False, encoding='utf-8-sig')
print(f"任务②结果已保存到: {output_file_2}")

# ===========================================
# 汇总信息
# ===========================================
print("\n" + "="*50)
print("抽样完成！")
print("="*50)
print(f"任务①：从 doubao 数据中抽取 {sample_count_doubao} 个 prompt，共 {len(doubao_result)} 行")
print(f"        保存到: {output_file_1}")
print(f"任务②：从 google 数据中抽取 {sample_count_google} 个 prompt，每个随机选 1 个 query，共 {len(google_result)} 行")
print(f"        保存到: {output_file_2}")
