from src.utils.structToDB.process_xlsx_xlsm import ProcessXLSX
process = ProcessXLSX()
file_path = "Sevensix_dropbox/機密レベル2/営業本部/営業活動/営業報告書/20250602_DMM_大城様_確度進捗_大久保.xlsm"
extracted = process.extract_meeting_report(file_path)
print(extracted)