import pandas as pd
import sys
from data_processor import load_data


def safe_print(text: str) -> None:
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode(enc, errors='replace').decode(enc, errors='replace'))

df = load_data(min_reviews=200)

print('Columns:', df.columns.tolist())
print('\nDtypes:')
print(df.dtypes)
print('\nFirst 5 rows:')
safe_print(df.head().to_string())
print('\nSample tags values:')
for i in range(3):
    tags_val = df.iloc[i]['tags_str']
    if pd.notna(tags_val):
        print(f'Row {i}: {str(tags_val)[:200]}...')
    else:
        print(f'Row {i}: NaN')
print('\nReviews stats:')
print(df['reviews'].describe())
