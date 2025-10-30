import pandas as pd
import psycopg2
from pandas.testing import assert_frame_equal

file_path = "/Users/pavelkokoshko/Desktop/db_assignment_files/Lab2-ExpectedResults/2.txt"

df_txt = pd.read_csv(file_path, sep="|", engine="python", skipinitialspace=True, dtype=str)
df_txt = df_txt.dropna(axis=1, how="all")         
df_txt = df_txt.apply(lambda c: c.str.strip())     
df_txt = df_txt.drop(0, errors="ignore")          
first_col = df_txt.columns[0]

df_txt = df_txt[~df_txt[first_col].str.match(r"^\(\d+\s+rows\)$", na=False)]
df_txt = df_txt.reset_index(drop=True)

conn = psycopg2.connect(
    host="", port=, dbname="", user="", password=""
)

query = """PLACE FOR YOUR QUERY HERE"""  

df_sql = pd.read_sql_query(query, conn).astype(str).apply(lambda c: c.str.strip())
conn.close()
df_sql = df_sql.reset_index(drop=True)

if df_txt.shape != df_sql.shape:
    print("Разный размер таблиц")
    print(f"txt:  {df_txt.shape}, sql: {df_sql.shape}")
else:
    try:
        assert_frame_equal(df_txt, df_sql, check_dtype=False, check_like=False)
        print("Полное совпадение: и порядок, и значения.")
    except AssertionError as e:
        print("Таблицы различаются (учитывая порядок).")
        neq_mask = (df_txt.values != df_sql.values).any(axis=1)
        bad_idx = list(pd.Index(range(len(df_txt)))[neq_mask])

        print(f"Первых 10 несовпадений (из {len(bad_idx)}):")
        for i in bad_idx[:10]:
            start = max(0, i-2); end = min(len(df_txt), i+3)
            print(f"\n--- mismatch at row {i} ---")
            print("TXT rows:")
            print(df_txt.iloc[start:end].to_string(index=True))
            print("SQL rows:")
            print(df_sql.iloc[start:end].to_string(index=True))
        if bad_idx:
            i = bad_idx[0]
            row_diff = pd.DataFrame({
                "col": df_txt.columns,
                "txt": df_txt.iloc[i].values,
                "sql": df_sql.iloc[i].values,
                "equal": df_txt.iloc[i].values == df_sql.iloc[i].values
            })
            print("\nПодробно по первой отличающейся строке:")
            print(row_diff.to_string(index=False))
