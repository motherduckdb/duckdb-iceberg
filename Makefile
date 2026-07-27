.PHONY: fixture fixture-latest gravitino lakekeeper polaris nessie

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
REST_CATALOG_CODEGEN_PYTHON := $(PROJ_DIR)duckdb/.cache/format-venv/bin/python

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

ifeq (${EQUALITY_DELETE_WRITES_ENABLED}, 1)
	EXT_FLAGS:=${EXT_FLAGS} -DICEBERG_ENABLE_EQUALITY_DELETE_WRITES=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

include make/util.mk
include make/catalogs/fixture.mk
include make/catalogs/gravitino.mk
include make/catalogs/lakekeeper.mk
include make/catalogs/nessie.mk
include make/catalogs/polaris.mk

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

.PHONY: rest-catalog-codegen-tools format-rest-catalog-code generate-rest-catalog-code

rest-catalog-codegen-tools:
	$(MAKE) -C duckdb parser-grammar-tools

format-rest-catalog-code: rest-catalog-codegen-tools
	$(REST_CATALOG_CODEGEN_PYTHON) duckdb/scripts/format.py src/include/rest_catalog/objects --fix --noconfirm
	$(REST_CATALOG_CODEGEN_PYTHON) duckdb/scripts/format.py src/rest_catalog/objects --fix --noconfirm

generate-rest-catalog-code: rest-catalog-codegen-tools
	$(REST_CATALOG_CODEGEN_PYTHON) scripts/generate_cpp_code.py
	$(REST_CATALOG_CODEGEN_PYTHON) duckdb/scripts/format.py src/include/rest_catalog/objects --fix --noconfirm
	$(REST_CATALOG_CODEGEN_PYTHON) duckdb/scripts/format.py src/rest_catalog/objects --fix --noconfirm
