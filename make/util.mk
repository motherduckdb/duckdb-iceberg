ACTIVE_CATALOG_FILE := .catalogs/.active_catalog

# Rewrites compose image references to $(OCI_REGISTRY_MIRROR); no-op when unset.
MIRROR_COMPOSE_IMAGES := scripts/mirror_compose_images.sh

# Stops whatever catalog is currently marked as active
define stop_active_catalog
	@if [ -f "$(ACTIVE_CATALOG_FILE)" ]; then \
		active=$$(cat $(ACTIVE_CATALOG_FILE)); \
		echo "Stopping active catalog: $$active"; \
		$(MAKE) $${active}-stop; \
	fi
	@rm -f $(ACTIVE_CATALOG_FILE)
endef

# Usage: $(call set_active_catalog,<name>)
define set_active_catalog
	@mkdir -p $(dir $(ACTIVE_CATALOG_FILE))
	@echo "$(1)" > $(ACTIVE_CATALOG_FILE)
endef