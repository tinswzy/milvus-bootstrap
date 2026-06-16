from .base import BaseServiceDriver, ServiceDriver
from .etcd import EtcdDriver

# kind -> driver class. Unlisted kinds fall back to the profile-driven base.
DRIVER_CLASSES: dict[str, type[ServiceDriver]] = {
    "etcd": EtcdDriver,
}

__all__ = ["ServiceDriver", "BaseServiceDriver", "EtcdDriver", "DRIVER_CLASSES"]
