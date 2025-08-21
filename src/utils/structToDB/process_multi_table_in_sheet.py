import pandas as pd

def read_multi_table_excel(file_path, sheet_name="18期売上台帳", key_column="受注番号"):
    # Step 1: Read raw sheet without headers
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Drop empty rows/columns first to speed up
    df = df.dropna(how="all").dropna(axis=1, how="all")
    tables = {}
    i = 0
    row_idx = 0

    while row_idx < len(df):
        row = df.iloc[row_idx]
        # Detect header row by checking if key_column exists in this row
        if row.astype(str).str.contains(key_column).any():
            header_row = row.astype(str).tolist()

            # Find key column index safely
            key_col_idx = None
            for idx, val in enumerate(header_row):
                if key_column in val:
                    key_col_idx = idx
                    break
            if key_col_idx is None:
                row_idx += 1
                continue  # should not happen

            # Determine end of table by checking key column empty
            end_idx = row_idx + 1
            while end_idx < len(df):
                key_cell = df.iloc[end_idx, key_col_idx]
                if pd.isna(key_cell) or str(key_cell).strip() == "":
                    break
                end_idx += 1

            # Extract block
            block = df.iloc[row_idx:end_idx].reset_index(drop=True)

            # --- Set headers ---
            header = block.iloc[0].fillna("").astype(str).tolist()
            seen = {}
            unique_headers = []
            for col in header:
                if col in seen:
                    seen[col] += 1
                    unique_headers.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    unique_headers.append(col)

            block.columns = unique_headers
            block = block.drop(0).reset_index(drop=True)

            # Only keep block if it has data
            if not block.empty:
                table_heading = f"Table_{i+1}"
                block["__TableBlock__"] = i
                tables[table_heading] = block
                print(f"{table_heading} shape:", block.shape)
                i += 1

            row_idx = end_idx
        else:
            row_idx += 1

    if not tables:
        raise ValueError("No tables were found. Check key_column name.")

    full_table = pd.concat(tables.values(), ignore_index=True)
    if "" in full_table.columns:
        full_table.drop(columns=[""],inplace=True)


    return full_table



