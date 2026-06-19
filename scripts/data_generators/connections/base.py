from typing import Dict, Type


class IcebergConnection:
    registry: Dict[str, Type['IcebergConnection']] = {}

    def __init__(self, name: str, catalog: str):
        self.name = name
        self.catalog = catalog

    @classmethod
    def register(cls, name: str):
        def decorator(subclass):
            cls.registry[name] = subclass
            return subclass

        return decorator

    @classmethod
    def get_class(cls, name: str):
        if name not in cls.registry:
            raise KeyError(f"Connection '{name}' is not registered")
        return cls.registry[name]

    def close(self) -> None:
        spark = getattr(self, "con", None)
        if spark is not None and hasattr(spark, "stop"):
            spark.stop()
