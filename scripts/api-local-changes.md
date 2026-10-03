# Local changes to `api.yaml`

`api.yaml` is vendored from the Apache Iceberg REST Catalog OpenAPI
specification. After replacing it with a newer upstream version, check whether
upstream contains the corrections below and reinstate any that are still
missing before regenerating the C++ REST objects.

## Primitive null values

Iceberg primitive values can be JSON `null`. Add a schema for that value:

```yaml
NullTypeValue:
  type: null
  nullable: true
  example: null
```

Add it as the first variant of `PrimitiveTypeValue.oneOf`:

```yaml
- $ref: '#/components/schemas/NullTypeValue'
```

Without this variant, generated expression and metrics objects cannot parse a
null primitive value.

## Boolean constant predicates (now upstream)

Upstream's `Predicate.oneOf` now includes an inline boolean variant as well
as the deprecated `TrueExpression` and `FalseExpression` object variants.
The former local `BooleanExpression` patch is no longer needed. Keep coverage
for canonical scan responses such as `"residual-filter": true` when bumping
the spec.

## Deprecated V1 table metadata

The REST OpenAPI models only the current table metadata representation, but
the same generated objects also parse metadata JSON files. Keep the deprecated
V1 `schema`, `partition-spec`, and `snapshot.manifests` fields in the vendored
schema, and do not require `snapshot.manifest-list`, so standalone V1 metadata
files can be read.

## Regeneration

After refreshing and correcting `api.yaml`, regenerate and format the REST
objects, then run the generator tests:

```shell
make generate-rest-catalog-code
python3 -m pytest -q test/scripts/test_openapi_codegen.py
```
