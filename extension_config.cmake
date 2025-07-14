# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)

duckdb_extension_load(tpch)
duckdb_extension_load(icu)

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 4ffd40243802a7b69be7231c74abd71eb1497c52
)

if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/tishj/duckdb_aws
            GIT_TAG 70a6ca678f7e126b233abc93bec163c0bdb9779e
    )
endif ()
endif()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG da2821906eb42f7255d969be3e073bc1b45a71a8
        INCLUDE_DIR extension/httpfs/include
)
