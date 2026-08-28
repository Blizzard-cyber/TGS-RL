"""SQLite-backed durable repositories for runtime and experiment surfaces."""

from tgsrl_runtime.storage.store_base import SQLiteStoreBase
from tgsrl_runtime.storage.store_gateway import SQLiteGatewayStoreMixin
from tgsrl_runtime.storage.store_maintenance import SQLiteMaintenanceMixin
from tgsrl_runtime.storage.store_paging import SQLitePagingMixin
from tgsrl_runtime.storage.store_runtime import SQLiteRuntimeStoreMixin


class SQLiteStore(
    SQLiteGatewayStoreMixin,
    SQLiteRuntimeStoreMixin,
    SQLiteMaintenanceMixin,
    SQLitePagingMixin,
    SQLiteStoreBase,
):
    """Durable sqlite compatibility facade composed from focused storage mixins."""
