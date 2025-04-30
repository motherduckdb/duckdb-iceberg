import os
import pkgutil
import importlib


# --- Dynamic importing logic to auto-register all tests ---
def _import_all_submodules():
    package_dir = os.path.dirname(__file__)
    for finder, name, ispkg in pkgutil.iter_modules([package_dir]):
        if ispkg:
            importlib.import_module(f".{name}", __name__)


_import_all_submodules()

from scripts.data_generators.tests.base import IcebergTest
