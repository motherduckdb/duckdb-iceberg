# This file is included by DuckDB's build system. It specifies which extension to load
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb-avro
		GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
)

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)

duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 09f9b85b7ea1c5c4a14ebfb83dd8ac5c9d65a874
)

<<<<<<< HEAD
=======
duckdb_extension_load(avro
	LOAD_TESTS
	GIT_URL https://github.com/duckdb/duckdb-avro
	GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
)
>>>>>>> d96105af (update for v1.4.0 DuckLake)

if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 812ce80fde0bfa6e4641b6fd798087349a610795
    )
endif ()
endif()
# This file is included by DuckDB's build system. It specifies which extension to load
duckdb_extension_load(avro
		LOAD_TESTS
		GIT_URL https://github.com/duckdb/duckdb-avro
		GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
)

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)

duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG e0a7b6ed5f2e6a52424d66fff0f91c87bc309022
)


if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 880da03202acc973d6ee7f3a0423dae5a6dea83b
    )
endif ()
endif()

