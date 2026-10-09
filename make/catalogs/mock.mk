.PHONY: mock mock-stop test_mock test_mock_release test_mock_debug test_mock_reldebug test_mock_relassert

MOCK_TEST_BINARY ?= $(PROJ_DIR)build/release/test/unittest
MOCK_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*

mock: mock-stop
	$(call stop_active_catalog)
	python3 -m scripts.mock_rest_catalog.lifecycle start
	$(call set_active_catalog,mock)

mock-stop:
	python3 -m scripts.mock_rest_catalog.lifecycle stop
	@if [ -f "$(ACTIVE_CATALOG_FILE)" ] && [ "$$(cat "$(ACTIVE_CATALOG_FILE)")" = mock ]; then \
		rm -f "$(ACTIVE_CATALOG_FILE)"; \
	fi

test_mock_release test_mock_debug test_mock_reldebug test_mock_relassert:
	$(MAKE) test_mock MOCK_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_mock_%,%,$@)/test/unittest"

test_mock:
	@set -e; \
	if [ "$(SKIP_TESTS)" = "1" ]; then echo "Mock catalog tests are skipped."; exit 0; fi; \
	trap '$(MAKE) mock-stop' EXIT; \
	$(MAKE) mock; \
	"$(MOCK_TEST_BINARY)" --test-config "$$(scripts/catalog_test_config.sh)" \
		"$(MOCK_TEST_FILTER)" "exclude:*.test_slow"
