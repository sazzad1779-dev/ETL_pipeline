from dataclasses import dataclass


@dataclass
class DirectoryConfig:
    dir_path: str
    collection_name: str
    level: int