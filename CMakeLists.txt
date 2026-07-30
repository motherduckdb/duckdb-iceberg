cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ standard")
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)

# Compile-time toggle for the experimental, testing-only Iceberg equality-delete WRITE path.
# Off by default: the shipped extension cannot create equality-delete files. Enable it to
# generate equality-delete files locally for testing the equality-delete READ path:
#   cmake ... -DICEBERG_ENABLE_EQUALITY_DELETE_WRITES=ON
option(ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
       "Compile in the experimental Iceberg equality-delete write path (testing only)" OFF)
if(ICEBERG_ENABLE_EQUALITY_DELETE_WRITES)
	add_definitions(-DICEBERG_ENABLE_EQUALITY_DELETE_WRITES)
endif()

# Resolve dependencies before descending into src/ so that every per-directory OBJECT library
# inherits the include directories it needs (these commands set directory-scoped properties that
# are inherited by subdirectories added afterwards).
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    find_package(CURL REQUIRED)
    find_package(AWSSDK REQUIRED COMPONENTS core sso sts)
    include_directories(${CURL_INCLUDE_DIRS})
    include_directories(${AWSSDK_INCLUDE_DIRS})
endif()

# Roaring is installed via vcpkg and used here
find_package(roaring CONFIG REQUIRED)
# Propagate roaring's usage requirements (its headers) to every OBJECT library under src/.
link_libraries(roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

# Reset the TARGET_NAME, the AWS find_package build could bleed into our build -
# overriding `TARGET_NAME`
set(TARGET_NAME iceberg)

# Each directory under src/ contributes its sources as an OBJECT library and appends them to
# ALL_OBJECT_FILES (see the per-directory CMakeLists.txt files).
add_subdirectory(src)

add_library(${EXTENSION_NAME} STATIC ${ALL_OBJECT_FILES})

set(PARAMETERS "-warnings")

if(EMSCRIPTEN)
  set (DUCKDB_EXTENSION_ICEBERG_LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-mqtt.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-kinesis.a;../../vcpkg_installed/wasm32-emscripten/lib/liblzma.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-auth.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-s3.a;../../vcpkg_installed/wasm32-emscripten/lib/libroaring.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-cal.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-sdkutils.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sso.a;../../vcpkg_installed/wasm32-emscripten/lib/libs2n.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-common.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-checksums.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-sts.a;../../vcpkg_installed/wasm32-emscripten/lib/libsnappy.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-compression.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-cognito-identity.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-crt-cpp.a;../../vcpkg_installed/wasm32-emscripten/lib/libssl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-event-stream.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-core.a;../../vcpkg_installed/wasm32-emscripten/lib/libcrypto.a;../../vcpkg_installed/wasm32-emscripten/lib/libz.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-http.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-dynamodb.a;../../vcpkg_installed/wasm32-emscripten/lib/libcurl.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-c-io.a;../../vcpkg_installed/wasm32-emscripten/lib/libaws-cpp-sdk-identity-management.a;../../vcpkg_installed/wasm32-emscripten/lib/libjansson.a;../../third_party/mbedtls/libduckdb_mbedtls.a")
endif()


build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    # AWS SDK FROM vcpkg
    target_include_directories(${EXTENSION_NAME}
                            PUBLIC $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
    target_link_libraries(${EXTENSION_NAME} PUBLIC ${AWSSDK_LINK_LIBRARIES})
    target_include_directories(${TARGET_NAME}_loadable_extension
                            PRIVATE $<BUILD_INTERFACE:${AWSSDK_INCLUDE_DIRS}>)
    target_link_libraries(${TARGET_NAME}_loadable_extension
                        ${AWSSDK_LINK_LIBRARIES})

    # Link dependencies into extension
    target_link_libraries(${EXTENSION_NAME} PUBLIC ${CURL_LIBRARIES})
    target_link_libraries(${TARGET_NAME}_loadable_extension ${CURL_LIBRARIES})
endif()

# Link Roaring library
target_link_libraries(${EXTENSION_NAME} PUBLIC roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
