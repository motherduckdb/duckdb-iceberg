.PHONY: mock mock-stop

mock: mock-stop
	$(call stop_active_catalog)
	python3 -m scripts.mock_rest_catalog.lifecycle start
	$(call set_active_catalog,mock)

mock-stop:
	python3 -m scripts.mock_rest_catalog.lifecycle stop
	@if [ -f "$(ACTIVE_CATALOG_FILE)" ] && [ "$$(cat "$(ACTIVE_CATALOG_FILE)")" = mock ]; then \
		rm -f "$(ACTIVE_CATALOG_FILE)"; \
	fi
