import pandas as pd
import psycopg2
from pandas.testing import assert_frame_equal
import sys, time, threading


class Spinner:
    def __init__(self, text="Подождите..."):
        self.text = text
        self._done = False
        self._thr = threading.Thread(target=self._spin, daemon=True)

    def _spin(self):
        symbols = "|/-\\"
        i = 0
        while not self._done:
            sys.stdout.write(f"\r{self.text} {symbols[i % len(symbols)]}")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
        sys.stdout.write("\r" + " " * (len(self.text) + 4) + "\r")
        sys.stdout.flush()

    def start(self):
        self._thr.start()

    def stop(self, final_text=None):
        self._done = True
        self._thr.join()
        if final_text:
            print(final_text)

file_path = "File path to .txt file"

df_txt = pd.read_csv(file_path, sep="|", engine="python", skipinitialspace=True, dtype=str)
df_txt = df_txt.dropna(axis=1, how="all")
df_txt = df_txt.apply(lambda c: c.str.strip())
df_txt = df_txt.drop(0, errors="ignore")
first_col = df_txt.columns[0]

df_txt = df_txt[~df_txt[first_col].str.match(r"^\(\d+\s+rows\)$", na=False)]
df_txt = df_txt.reset_index(drop=True)

def normalize_numeric_cols(df):
    for col in df.columns:
        try:
            df[col] = df[col].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) and x != '' else x)
        except ValueError:
            pass
    return df

spin = Spinner("Соединяемся с базой и выполняем SQL-запрос...")
spin.start()
try:

    conn = psycopg2.connect(
        host="", port="", dbname="", user="", password=""
    )

    query = """YOUR QUERY HERE"""

    df_sql = pd.read_sql_query(query, conn).astype(str).apply(lambda c: c.str.strip())
finally:
    try:
        conn.close()
    except Exception:
        pass
    spin.stop("SQL-запрос выполнен.")

df_sql = df_sql.reset_index(drop=True)

spin = Spinner("Нормализуем и сравниваем таблицы... ")
spin.start()
try:
    df_txt = normalize_numeric_cols(df_txt.copy())
    df_sql = normalize_numeric_cols(df_sql.copy())

    if df_txt.shape != df_sql.shape:
        print("\nРазный размер таблиц")
        print(f"txt:  {df_txt.shape}, sql: {df_sql.shape}")
    else:
        try:
            assert_frame_equal(df_txt, df_sql, check_dtype=False, check_like=False)
            print("\nПолное совпадение: и порядок, и значения.")
        except AssertionError as e:

            neq_mask = (df_txt.values != df_sql.values).any(axis=1)
            bad_idx = list(pd.Index(range(len(df_txt)))[neq_mask])

            if len(bad_idx) == 0:
                print("\nТаблицы совпадают по значениям (возможны разные типы внутренние данных, инкодинг итд).")

            else:
                print("\nТаблицы различаются (учитывая порядок).")

                print(f"\nПервых 10 несовпадений (из {len(bad_idx)}):")
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
finally:
    try:
        spin.stop(None)
    except Exception:
        pass
