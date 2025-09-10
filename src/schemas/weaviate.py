from weaviate.classes.config import DataType

PRODUCT_SCHEMA= [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "image_urls", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "youtube_urls", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "level", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "origin", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "chunk_index", "data_type": DataType.INT, "vectorize_property": False}
]
DEFAULT_SCHEMA = [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "level", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "origin", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "chunk_index", "data_type": DataType.INT, "vectorize_property": False},

]