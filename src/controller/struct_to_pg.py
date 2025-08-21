from pathlib import Path
from src.utils.structToDB.process import StructToProcess

class StructuredToPGController:
    def __init__(self, files_dir: list = None, use_dask: bool = False, threshold_mb: int = 100,skip_row:int=0):
        """
        db_url: PostgreSQL connection string (e.g. "postgresql+psycopg2://user:password@localhost:5432/mydb")
        
        """
        self.files = files_dir
        self.use_dask = use_dask
        self.threshold_mb = threshold_mb
        self.skip_row = skip_row
        self.process = StructToProcess()

    def process_files(self):
        for full_path in self.files:
            file_lower = full_path.lower()
            table_name = Path(file_lower).stem
            file_size_mb = self.process._get_file_size_mb(full_path)
            try:
                if file_lower.endswith(".csv"):
                    if self.use_dask or file_size_mb > self.threshold_mb:
                        self.process._load_with_dask(full_path, table_name)
                    else:
                        self.process._load_with_pandas(full_path, table_name)
                elif file_lower.endswith((".xlsx", ".xlsm")):
                    self.process._load_xlsx_with_pandas(full_path, table_name)
                else:
                    print(f"⚠️ Skipping unsupported file: {full_path}")
            except Exception as e:
                print(f"❌ Error processing {full_path}: {e}")

    def process_individual_file(self, file_path=None):
        file_lower = file_path.lower()
        table_name = Path(file_lower).stem
        file_size_mb = self.process._get_file_size_mb(file_path)
        try:
            if file_lower.endswith(".csv"):
                if self.use_dask or file_size_mb > self.threshold_mb:
                    self.process._load_with_dask(file_path, table_name)
                else:
                    self.process._load_with_pandas(file_path, table_name)
            elif file_lower.endswith((".xlsx", ".xlsm")):
                self.process._load_xlsx_with_pandas(file_path, table_name)
            else:
                print(f"⚠️ Skipping unsupported file: {file_path}")
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
    

if __name__ == "__main__":

    loader = StructuredToPGController(
    files_dir=["Sevensix_dropbox/機密レベル3/営業本部/メーカー別/Innolight/Innolight Price List.xlsx","Sevensix_dropbox/機密レベル3/営業本部/メーカー別/Polatis(Huber + Shuner)/Polatis光SW価格表.xlsx"],
    use_dask=False,
    )
    loader.process_files()