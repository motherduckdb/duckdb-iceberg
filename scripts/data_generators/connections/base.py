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
            print(f"'{name}' not in registry, exiting!")
            exit(1)
        return cls.registry[name]
