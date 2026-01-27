from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.weaviate_controller import WeaviateController
from src.schemas.weaviate import DEFAULT_SCHEMA1

level = "1",
origin = "master_data",
embedding = "openai",
weaviate_client = WeaviateController(
            collection_name="Master_data",
            # embedding_provider=embedding,
            properties=DEFAULT_SCHEMA1,
            collection_delete=False
        )

import pandas as pd

# import pandas as pd

def csv_to_chunks_and_sources(
    csv_path: str,
    url_column: str = "URL",
    encoding: str = "utf-8-sig"
):
    df = pd.read_csv(csv_path, encoding=encoding)

    # Pre-compute column indices (fast lookup)
    columns = list(df.columns)
    url_idx = columns.index(url_column)

    chunks = []
    sources = []

    for row in df.itertuples(index=False):
        lines = []
        source_url = None

        for idx, val in enumerate(row):
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

weaviate_client.insert_data_from_lists(
            content=chunks,
            source=sources,
            # level=[level] * len(chunks),
            # origin=[origin] * len(chunks),
            # chunk_index=[0 for i in range(len(chunks))]
        )


# processor.run_product_spec()
# processor.retrieve_data_by_field(
#     field_list=["content", "source","level"],
#     limit=5
# )
# print(" near searach\n"," *"*50)
# processor.query_data("対象サイズ")
# print(" hybrid search\n"," *"*50)
# processor.query_data_hybrid("対象サイズ")