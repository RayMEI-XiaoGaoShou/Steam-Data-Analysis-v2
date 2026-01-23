import pandas as pd

df = pd.read_csv(r'C:\Users\happyelements\Desktop\Steam 数据分析\data\bestSelling_games.csv', encoding='latin-1')
# 删除空列
df = df.drop(columns=['Unnamed: 4', 'Unnamed: 5'], errors='ignore')

print('Columns:', df.columns.tolist())
print('\nDtypes:')
print(df.dtypes)
print('\nFirst 5 rows:')
print(df.head().to_string())
print('\nSample tags values:')
for i in range(3):
    tags_val = df.iloc[i]['user_defined_tags']
    if pd.notna(tags_val):
        print(f'Row {i}: {str(tags_val)[:200]}...')
    else:
        print(f'Row {i}: NaN')
print('\nReviews stats:')
print(df['all_reviews_number'].describe())
