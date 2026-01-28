from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.weaviate_controller import WeaviateController
from src.schemas.weaviate import DEFAULT_SCHEMA1

level = "1",
origin = "master_data",
embedding = "openai",
weaviate_client = WeaviateController(
            collection_name="Master_data_v2",
            # embedding_provider=embedding,
            properties=DEFAULT_SCHEMA1,
            collection_delete=False
        )

import pandas as pd

# import pandas as pd

# def csv_to_chunks_and_sources(
#     csv_path: str,
#     url_column: str = "URL",
#     encoding: str = "utf-8-sig"
# ):
#     df = pd.read_csv(csv_path, encoding=encoding)

#     # Pre-compute column indices (fast lookup)
#     columns = list(df.columns)
#     url_idx = columns.index(url_column)

#     chunks = []
#     sources = []

#     for row in df.itertuples(index=False):
#         lines = []
#         source_url = None

#         for idx, val in enumerate(row):
#             if val is None or (isinstance(val, float) and pd.isna(val)):
#                 continue

#             val = str(val).strip()
#             if not val:
#                 continue

#             if idx == url_idx:
#                 source_url = val
#             else:
#                 lines.append(f"{columns[idx]}: {val}")

#         if lines:
#             chunks.append("\n".join(lines))
#             sources.append(source_url)

#     return chunks, sources


def csv_to_chunks_and_sources(
    csv_path: str,
    url_column: str = "URL",
    demo_column: str = "デモ機貸出し",
    encoding: str = "utf-8-sig"
):
    df = pd.read_csv(csv_path, encoding=encoding)

    columns = list(df.columns)

    if url_column not in columns:
        raise ValueError(f"'{url_column}' column not found in CSV")

    url_idx = columns.index(url_column)
    demo_idx = columns.index(demo_column) if demo_column in columns else None

    chunks = []
    sources = []

    for row in df.itertuples(index=False):
        lines = []
        source_url = None

        for idx, val in enumerate(row):
            # Handle demo column explicitly
            if idx == demo_idx:
                if val is None or (isinstance(val, float) and pd.isna(val)) or not str(val).strip():
                    val = "利用不可"
                elif str(val).strip() == "あり":
                    val = "利用可能"
                else:
                    val = str(val).strip()

            # Skip other empty values
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue

            val = str(val).strip()
            if not val:
                continue

            if idx == url_idx:
                source_url = val
            else:
                lines.append(f"{columns[idx]}: {val}")

        if lines:
            chunks.append("\n".join(lines))
            sources.append(source_url)

    return chunks, sources


# ---------- Usage ----------
csv_path = "202601_製品マスタ - 製品マスタ.csv"
chunks, sources = csv_to_chunks_and_sources(csv_path)

print(chunks[:2])  # Print first 2 chunks for verification
print(sources[:2])  # Print first 2 sources for verification

# weaviate_client.insert_data_from_lists(
#             content=chunks,
#             source=sources,
#         )