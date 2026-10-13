import os
import pkgutil
import importlib


# --- Dynamic importing logic to auto-register all tests ---
def _import_all_submodules():
    package_dir = os.path.dirname(__file__)
    for _, name, _ in pkgutil.walk_packages([package_dir], prefix=f"{__name__}."):
        importlib.import_module(name)


_import_all_submodules()

from scripts.data_generators.tests.base import IcebergTest
