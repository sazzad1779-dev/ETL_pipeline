import os
from typing import List


class FileLoader:
    def __init__(self, directory_paths: List[str], allowed_extensions: List[str]):
        """
        Initialize FileLoader with a list of directory paths and allowed file extensions.

        :param directory_paths: List of folder paths to scan.
        :param allowed_extensions: List of allowed file extensions (e.g. [".txt", ".pdf"])
        """
        self.directory_paths = directory_paths
        self.allowed_extensions = [ext.lower() for ext in allowed_extensions]

    def is_valid_extension(self, filename: str) -> bool:
        """
        Check if a file has a valid extension.

        :param filename: Name of the file to check.
        :return: True if extension is valid, else False.
        """
        _, ext = os.path.splitext(filename)
        return ext.lower() in self.allowed_extensions

    def load_files(self) -> List[str]:
        """
        Load all files from the given directories matching the allowed extensions.

        :return: List of matching file paths.
        """
        matched_files = []

        for directory in self.directory_paths:
            try:
                if not os.path.exists(directory):
                    print(f"⚠️ Directory not found: {directory}")
                    continue

                for root, _, files in os.walk(directory):
                    for file in files:
                        try:
                            if self.is_valid_extension(file):
                                matched_files.append(os.path.join(root, file))
                        except Exception as file_error:
                            print(f"⚠️ Skipping file due to error: {file} — {file_error}")

            except Exception as e:
                print(f"❌ Error while scanning directory {directory}: {e}")
        
        return matched_files
