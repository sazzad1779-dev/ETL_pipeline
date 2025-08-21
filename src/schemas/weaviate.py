from weaviate.classes.config import DataType
property_config = [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
    ]


Product_collection_property_config = [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "image_urls", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "youtube_urls", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "level", "data_type": DataType.INT, "vectorize_property": False}
]
Product_collection_tenancy_list = [
    # Tenant(
    #     name="business_cluster",
    #     activity_status=TenantActivityStatus.INACTIVE
    # ),
]
default_schema = [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
    {"name": "level", "data_type": DataType.INT, "vectorize_property": False},
]
Regulation_collection_property_config = [
    {"name": "content", "data_type": DataType.TEXT, "vectorize_property": True},
    {"name": "source", "data_type": DataType.TEXT, "vectorize_property": False},
]