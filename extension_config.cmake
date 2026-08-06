# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/tishj/duckdb_avro
  GIT_TAG 3cac15462f91e5e9152bc32aaffba522b53c961f
  SUBMODULES "third_party/avro-c"
)
endif()

# Extension from this repo
if (DONT_LINK OR "$ENV{DONT_LINK}")
  set(ICEBERG_DONT_LINK "DONT_LINK")
else()
  set(ICEBERG_DONT_LINK "")
endif()


duckdb_extension_load(json)
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    ${ICEBERG_DONT_LINK}
)

if (NOT EMSCRIPTEN)
  duckdb_extension_load(tpch)
  duckdb_extension_load(icu)
#  duckdb_extension_load(ducklake
#        LOAD_TESTS
#        GIT_URL https://github.com/duckdb/ducklake
#        GIT_TAG a92abf755a7b4e2f3e410f8b89c72b990a0698da
#)

  if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 7d6b5be7ad13977307f24ad96062fdb0cc9f371a
    )
  endif()
endif()
