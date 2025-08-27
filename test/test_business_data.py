from src.controller.postgres_controller import PostgresController
from src.controller.agentic_controller import AgenticExtractor
# from src.schemas.weaviate import 
from src.utils.file_loader import FileLoader
import os
directory_path = "Sevensix_dropbox/機密レベル3/企画管理本部/人事総務部/競合企業情報"
origin="s3_bucket"
level="3"
psql = PostgresController()
extractor = AgenticExtractor()
i=0
# Loop through all files in the directory (non-recursive)
for filename in os.listdir(directory_path):
    if filename.lower().endswith(".pdf"):

        file_path = os.path.join(directory_path, filename)
        print("Found PDF:", file_path)
        if file_path=="Sevensix_dropbox/機密レベル3/企画管理本部/人事総務部/競合企業情報/20241002_オプトサイエンス（東京商工リサーチ）.pdf":
             continue
        # result = extractor.parse_documents(file_path)
        #file_path="Sevensix_dropbox/機密レベル3/企画管理本部/人事総務部/競合企業情報/201846451ハナムラオ20241226_1.pdf"
        result = extractor.parse_documents(file_path)
        psql.create_tables()
        ser_result = result[0].extraction
        print(ser_result)
        # Insert the structured data into the Postgres database
        psql.insert_organization_person(ser_result,source=file_path,origin=origin,level=level)
        # if i>2:
        break
        # i+=1