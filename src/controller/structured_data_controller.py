from pathlib import Path
from src.utils.structToDB.process import StructToProcess
from src.utils.file_loader import FileLoader
class StructuredDataController:
    def __init__(self, files_dir: list = None,
                 use_dask: bool = False, allowed_extensions:list=[".csv",".xlsx",".xlsm"],level:str="1",origin:str="s3_bucket",skip_files:list=[],threshold_mb: int = 100, skip_row: int = 0):
        file_loader= FileLoader(files_dir,allowed_extensions=allowed_extensions)
        self.files  = file_loader.load_files()
        self.use_dask = use_dask
        self.threshold_mb = threshold_mb
        self.skip_row = skip_row
        self.process = StructToProcess(level,origin)
        self.skip_files = skip_files

    def process_files(self,):
        """
        Process files and return dictionary of processed DataFrames.
        {table_name: DataFrame}
        """
        processed_results = {}

        for full_path in self.files:
            file_lower = full_path.lower()
            table_name = Path(file_lower).stem
            if Path(full_path).name in self.skip_files :
                continue

            file_size_mb = self.process._get_file_size_mb(full_path)

            try:
                if file_lower.endswith(".csv"):
                    if self.use_dask or file_size_mb > self.threshold_mb:
                        dfs = self.process._load_with_dask(full_path, table_name)
                    else:
                        dfs = self.process._load_with_pandas(full_path, table_name)

                elif file_lower.endswith((".xlsx", ".xlsm")):
                    dfs = self.process._load_xlsx_with_pandas(full_path, table_name)

                else:
                    print(f"⚠️ Skipping unsupported file: {full_path}")
                    continue 
                if dfs:
                    processed_results.update(dfs)

            except Exception as e:
                print(f"❌ Error processing {full_path}: {e}")

        return processed_results
