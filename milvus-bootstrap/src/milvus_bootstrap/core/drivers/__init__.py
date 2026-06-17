from .base import BaseServiceDriver, ServiceDriver
from .etcd import EtcdDriver
from .milvus import MilvusDriver
from .minio import MinioDriver
from .woodpecker import WoodpeckerDriver

# kind -> driver class. Unlisted kinds fall back to the profile-driven base.
DRIVER_CLASSES: dict[str, type[ServiceDriver]] = {
    "etcd": EtcdDriver,
    "minio": MinioDriver,
    "woodpecker": WoodpeckerDriver,
    "milvus": MilvusDriver,
}

__all__ = [
    "ServiceDriver", "BaseServiceDriver",
    "EtcdDriver", "MinioDriver", "WoodpeckerDriver", "MilvusDriver", "DRIVER_CLASSES",
]
