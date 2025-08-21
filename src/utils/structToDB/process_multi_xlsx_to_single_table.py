import pandas as pd
import glob
import os

def combine_excel_files(folder_path: str, sheet_name: str = 0, skip_files: list = []) -> pd.DataFrame:
    # Find all Excel files in the folder
    files = glob.glob(f"{folder_path}/*.xlsx")
    
    df_list = []
    for file in files:
        file_name = os.path.basename(file)  # only file name
        if file_name in skip_files:         # compare names only
            print(f"Skipping {file_name}")
            continue
        try:
            df = pd.read_excel(file, sheet_name=sheet_name)
            df = df.dropna(how="all").dropna(axis=1, how="all")
            
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
    
    if not df_list:
        print("⚠️ No valid DataFrames to concatenate.")
        return pd.DataFrame()  # return empty DataFrame
    
    # Combine into one DataFrame
    full_table = pd.concat(df_list, ignore_index=True)
    if " " in full_table.columns or '　' in full_table.columns or "" in full_table.columns:
        full_table.drop(columns=['　'],inplace=True)
    return full_table

