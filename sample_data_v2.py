#!/usr/bin/env python3
"""
数据抽样脚本 v2：
要求：两个任务使用相同的 100 个 prompt

任务①：在 engine_type=doubao 的数据中，抽取这 100 个 prompt 对应的全部数据行
任务②：在 engine_type=google 的数据中，对这 100 个 prompt 各随机抽一个 query，
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

# 分别获取 doubao 和 google 数据
doubao_df = df[df['engine_type'] == 'doubao']
google_df = df[df['engine_type'] == 'google']

print(f"Doubao 数据: {len(doubao_df)} 行")
print(f"Google 数据: {len(google_df)} 行")

# 获取各自的唯一 prompt
doubao_prompts = set(doubao_df['prompt'].unique())
google_prompts = set(google_df['prompt'].unique())

print(f"Doubao 唯一 prompt: {len(doubao_prompts)} 个")
print(f"Google 唯一 prompt: {len(google_prompts)} 个")

# 找到两者共同的 prompt
common_prompts = list(doubao_prompts & google_prompts)
print(f"共同 prompt: {len(common_prompts)} 个")

# 从共同 prompt 中随机抽取 100 个
sample_count = min(100, len(common_prompts))
sampled_prompts = random.sample(common_prompts, sample_count)
print(f"抽取了 {sample_count} 个共同 prompt")

# ===========================================
# 任务①：doubao 数据 - 抽取这 100 个 prompt 对应的全部数据行
# ===========================================
doubao_result = doubao_df[doubao_df['prompt'].isin(sampled_prompts)]
print(f"\n任务①结果：{len(doubao_result)} 行数据")

output_file_1 = '/Users/miawu/Downloads/任务1_doubao_100prompts抽样.csv'
doubao_result.to_csv(output_file_1, index=False, encoding='utf-8-sig')
print(f"任务①结果已保存到: {output_file_1}")

# ===========================================
# 任务②：google 数据 - 对这 100 个 prompt 各随机抽一个 query
# ===========================================
sampled_queries = []
for prompt in sampled_prompts:
    # 获取该 prompt 下的所有 query
    prompt_queries = google_df[google_df['prompt'] == prompt]['query'].unique().tolist()
    # 随机选一个 query
    selected_query = random.choice(prompt_queries)
    sampled_queries.append((prompt, selected_query))

print(f"抽取了 {len(sampled_queries)} 个 (prompt, query) 组合")

# 获取这些 query 对应的所有数据行
google_result_list = []
for prompt, query in sampled_queries:
    matching_rows = google_df[(google_df['prompt'] == prompt) & (google_df['query'] == query)]
    google_result_list.append(matching_rows)

google_result = pd.concat(google_result_list, ignore_index=True)
print(f"任务②结果：{len(google_result)} 行数据")

output_file_2 = '/Users/miawu/Downloads/任务2_google_100prompts_随机query抽样.csv'
google_result.to_csv(output_file_2, index=False, encoding='utf-8-sig')
print(f"任务②结果已保存到: {output_file_2}")

# ===========================================
# 汇总信息
# ===========================================
print("\n" + "="*50)
print("抽样完成！")
print("="*50)
print(f"使用相同的 {sample_count} 个 prompt（doubao 和 google 的交集）")
print(f"任务①：doubao 数据，共 {len(doubao_result)} 行")
print(f"        保存到: {output_file_1}")
print(f"任务②：google 数据（每个 prompt 随机选 1 个 query），共 {len(google_result)} 行")
print(f"        保存到: {output_file_2}")
