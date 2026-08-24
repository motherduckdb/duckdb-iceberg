#!/usr/bin/env bash
#
# Rewrite `image:` references in docker-compose files to pull third-party images
# through the registry mirror named by OCI_REGISTRY_MIRROR (an ECR pull-through
# cache in CI) instead of anonymously from Docker Hub, quay.io or registry.k8s.io.
#
# Usage: scripts/mirror_compose_images.sh <compose-file> [<compose-file> ...]
#
# When OCI_REGISTRY_MIRROR is unset or empty this is a no-op, so local runs and
# environments without mirror access behave exactly as before. Rewriting is
# idempotent: references already pointing at the mirror are left alone.

set -euo pipefail

if [ -z "${OCI_REGISTRY_MIRROR:-}" ]; then
	exit 0
fi

if [ "$#" -eq 0 ]; then
	echo "usage: $0 <compose-file> [<compose-file> ...]" >&2
	exit 2
fi

# Tolerate a trailing slash in the configured mirror.
mirror=$(printf '%s' "$OCI_REGISTRY_MIRROR" | sed 's:/*$::')

for file in "$@"; do
	if [ ! -f "$file" ]; then
		echo "mirror_compose_images: skipping missing file $file" >&2
		continue
	fi

	tmp="$file.mirror.tmp"
	awk -v mirror="$mirror" -v out="$tmp" -v file="$file" '
		BEGIN { printf "" > out }

		function is_host(component) {
			return component ~ /[.:]/ || component == "localhost"
		}

		function dockerhub(rest) {
			# Bare official images live under library/ in the Docker Hub namespace.
			if (rest !~ /\//) {
				rest = "library/" rest
			}
			return mirror "/dockerhub/" rest
		}

		function map_ref(ref,   slash, host, rest) {
			if (ref == mirror || index(ref, mirror "/") == 1) {
				return ref
			}
			slash = index(ref, "/")
			if (slash == 0) {
				return dockerhub(ref)
			}
			host = substr(ref, 1, slash - 1)
			rest = substr(ref, slash + 1)
			if (!is_host(host)) {
				return dockerhub(ref)
			}
			if (host == "docker.io" || host == "index.docker.io") {
				return dockerhub(rest)
			}
			if (host == "quay.io") {
				return mirror "/quay/" rest
			}
			if (host == "registry.k8s.io" || host == "k8s.gcr.io") {
				return mirror "/k8s/" rest
			}
			return ref
		}

		# Compose values are often written as ${VAR:-some/image:tag}; rewrite the
		# default so the fallback image is mirrored too.
		function map_value(value,   inner, mapped) {
			if (value ~ /^\$\{[A-Za-z_][A-Za-z0-9_]*:-.*\}$/) {
				inner = substr(value, index(value, ":-") + 2)
				inner = substr(inner, 1, length(inner) - 1)
				mapped = map_ref(inner)
				if (mapped == inner) {
					return value
				}
				return substr(value, 1, index(value, ":-") + 1) mapped "}"
			}
			if (value ~ /^\$/) {
				return value
			}
			return map_ref(value)
		}

		{
			line = $0
			if (line ~ /^[[:space:]]*#/ || line !~ /^[[:space:]]*image:[[:space:]]*[^[:space:]]/) {
				print line > out
				next
			}

			match(line, /^[[:space:]]*image:[[:space:]]*/)
			prefix = substr(line, 1, RLENGTH)
			remainder = substr(line, RLENGTH + 1)

			# Split the scalar off any trailing content (e.g. a comment).
			if (match(remainder, /[[:space:]]/)) {
				value = substr(remainder, 1, RSTART - 1)
				suffix = substr(remainder, RSTART)
			} else {
				value = remainder
				suffix = ""
			}

			quote = ""
			if (length(value) > 1 && (substr(value, 1, 1) == "\"" || substr(value, 1, 1) == "'\''") && substr(value, length(value), 1) == substr(value, 1, 1)) {
				quote = substr(value, 1, 1)
				value = substr(value, 2, length(value) - 2)
			}

			mapped = map_value(value)
			if (mapped != value) {
				printf "mirror_compose_images: %s: %s -> %s\n", file, value, mapped
			}
			print prefix quote mapped quote suffix > out
		}
	' "$file"

	mv "$tmp" "$file"
done
