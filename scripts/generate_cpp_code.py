from parse_openapi_spec import (
    ResponseObjectsGenerator,
    Property,
    ArrayProperty,
    PrimitiveProperty,
    SchemaReferenceProperty,
    ObjectProperty,
)
import os
from typing import Dict, List, Tuple, Set, Optional, cast, Callable
from enum import Enum, auto
from dataclasses import dataclass, field

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
OUTPUT_HEADER_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')
OUTPUT_SOURCE_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'rest_catalog', 'objects')
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

CPP_KEYWORDS = {
    'namespace',
    'class',
    'template',
    'operator',
    'private',
    'public',
    'protected',
    'virtual',
    'default',
    'delete',
    'final',
    'override',
    'error',  # add 'error' to avoid conflicts with the 'error' variable in TryFromJSON
    'doc',  # add 'doc' to avoid conflicts with the 'doc' variable in StructField
}

SERIALIZATION_EXCLUDED = [
    'LiteralExpression',
    'UnaryExpression'
]

def to_snake_case(name: str):
    res = ''
    prev_was_lower = False
    for x in name:
        is_lower = x.islower()
        if not is_lower and prev_was_lower:
            res += '_'
        prev_was_lower = is_lower
        res += x.lower()
    return res


def safe_cpp_name(name: str) -> str:
    """Convert property name to safe C++ variable name."""
    name = name.replace('-', '_')
    if name in CPP_KEYWORDS:
        return '_' + name
    return name


HEADER_FORMAT = """
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
{ADDITIONAL_HEADERS}

using namespace duckdb_yyjson;

namespace duckdb {{
namespace rest_api_objects {{

{FORWARD_DECLARATIONS}

{CLASS_DECLARATION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

SOURCE_FORMAT = """
#include "rest_catalog/objects/{HEADER_NAME}.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {{
namespace rest_api_objects {{

{CLASS_DEFINITION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

CMAKE_LISTS_FORMAT = """
add_library(
	rest_catalog_objects
	OBJECT
{ALL_SOURCE_FILES}
)

set(ALL_OBJECT_FILES
    ${{ALL_OBJECT_FILES}} $<TARGET_OBJECTS:rest_catalog_objects>
    PARENT_SCOPE)
"""


@dataclass
class ParseInfo:
    """Data taken from the parser"""

    recursive_schemas: Set[str]
    schemas: dict
    parsed_schemas: Dict[str, Property]


@dataclass
class OneOf:
    """An option of the OneOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class AnyOf:
    """An option of the AnyOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class AllOf:
    """An option of the AllOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class RequiredProperty:
    """A property that is required to be present in the JSON"""

    # The variable name in the generated cpp code
    variable_name: str
    # The property name in the JSON code
    property_name: str
    body: List[str]
    default: Optional[List[str]]
    schema: Property  # Store the property schema for serialization


@dataclass
class OptionalProperty:
    """A property that is can or can't be present in the JSON"""

    # The variable name in the generated cpp code
    variable_name: str
    # The property name in the JSON code
    property_name: str
    body: List[str]
    schema: Property  # Store the property schema for serialization
    nullable: bool


@dataclass
class AdditionalProperty:
    """The additional (typed) properties not covered by the spec"""

    body: List[str]
    exclude_list: List[str] = field(default_factory=list)
    skip_if_excluded: List[str] = field(default_factory=list)
    schema: Optional[Property] = None  # Store the property schema for serialization


@dataclass
class CPPMember:
    """A generated C++ class member"""

    variable_name: str
    variable_type: str
    schema: Optional[Property]
    initializer: Optional[str] = None
    copy_guard: Optional[str] = None


@dataclass
class PrimitiveTypeMapping:
    conversion: str
    type_check: str
    cpp_type: str
    formats: Dict[str, "PrimitiveTypeMapping"] = field(default_factory=dict)


PRIMITIVE_TYPE_MAPPING = {
    'string': PrimitiveTypeMapping(type_check='yyjson_is_str', conversion='yyjson_get_str', cpp_type='string'),
    'integer': PrimitiveTypeMapping(
        type_check='yyjson_is_int',
        conversion='yyjson_get_int',
        cpp_type='int32_t',
        formats={
            'int64': PrimitiveTypeMapping(type_check='yyjson_is_sint', conversion='yyjson_get_sint', cpp_type='int64_t')
        },
    ),
    'boolean': PrimitiveTypeMapping(type_check='yyjson_is_bool', conversion='yyjson_get_bool', cpp_type='bool'),
    'number': PrimitiveTypeMapping(type_check='yyjson_is_num', conversion='yyjson_get_num', cpp_type='double'),
}


class CPPClass:
    def __init__(self, class_name, parse_info: ParseInfo):
        self.name = class_name
        self.parse_info = parse_info
        # The base classes that make up this class
        self.one_of: List[OneOf] = []
        self.all_of: List[AllOf] = []
        self.any_of: List[AnyOf] = []

        # Parsing code of the TryFromJSON method
        self.required_properties: Dict[str, RequiredProperty] = {}
        self.optional_properties: Dict[str, OptionalProperty] = {}
        self.additional_properties: Optional[AdditionalProperty] = None

        # Nested classes of this class (referenced by variables)
        self.nested_classes: Dict[str, "CPPClass"] = {}
        # (member) variables of the class
        self.variables: List[str] = []
        self.members: List[CPPMember] = []
        self.referenced_schemas: Set[str] = set()
        self.try_from_json_body: List[str] = []

    def add_member(
        self,
        variable_name: str,
        variable_type: str,
        schema: Optional[Property],
        initializer: Optional[str] = None,
        copy_guard: Optional[str] = None,
    ) -> None:
        initializer_text = f' = {initializer}' if initializer is not None else ''
        self.variables.append(f'\t{variable_type} {variable_name}{initializer_text};')
        self.members.append(
            CPPMember(
                variable_name=variable_name,
                variable_type=variable_type,
                schema=schema,
                initializer=initializer,
                copy_guard=copy_guard,
            )
        )

    def get_all_referenced_schemas(self) -> Set[str]:
        res = set()
        res.update(self.referenced_schemas)
        for item in self.nested_classes.values():
            res.update(item.get_all_referenced_schemas())
        return res

    def from_object_property(self, schema: ObjectProperty):
        assert schema.type == Property.Type.OBJECT
        object_property = cast(ObjectProperty, schema)

        # Parse any base classes required for the schema (anyOf, allOf, oneOf)
        self.generate_all_of(schema)
        self.generate_one_of(schema)
        self.generate_any_of(schema)

        inherited_properties = self.collect_all_of_property_names(schema)
        self.validate_polymorphic_property_ownership(schema, inherited_properties)

        required = object_property.required
        if not required:
            required = []
        remaining_properties = [
            x for x in object_property.properties
            if x not in required and x not in inherited_properties
        ]

        required_properties = {}
        optional_properties = {}
        for item in remaining_properties:
            optional_properties[item] = object_property.properties[item]
        for item in required:
            if item in inherited_properties:
                continue
            required_properties[item] = object_property.properties[item]

        self.generate_required_properties(self.name, required_properties)
        self.generate_optional_properties(self.name, optional_properties)
        self.generate_additional_properties(object_property.properties.keys(), object_property.additional_properties)

        res = []
        for _, item in self.required_properties.items():
            res.extend([f'\t{x}' for x in self.write_required_property(item)])
        for _, item in self.optional_properties.items():
            res.extend([f'\t{x}' for x in self.write_optional_property(item)])
        res.extend([f'\t{x}' for x in self.write_additional_properties()])
        self.try_from_json_body = res
        self.generate_nested_class_definitions()

    def from_array_property(self, schema: ArrayProperty):
        assert schema.type == Property.Type.ARRAY
        array_property = cast(ArrayProperty, schema)

        assert not array_property.all_of
        assert not array_property.one_of
        assert not array_property.any_of

        self.try_from_json_body = self.generate_array_loop('obj', 'value', array_property)

        nested_classes = self.generate_nested_class_definitions()

        variable_type = self.generate_variable_type(schema)
        self.add_member('value', variable_type, schema)

    def from_primitive_property(self, schema: PrimitiveProperty):
        assert not schema.all_of
        assert not schema.one_of
        assert not schema.any_of

        self.try_from_json_body = self.generate_assignment(schema, 'value', 'obj', True)

        variable_type = self.generate_variable_type(schema)
        self.add_member('value', variable_type, schema)

    def from_property(self, schema: Property) -> None:
        if schema.type == Property.Type.OBJECT:
            self.from_object_property(schema)
        elif schema.type == Property.Type.ARRAY:
            self.from_array_property(schema)
        elif schema.type == Property.Type.PRIMITIVE:
            self.from_primitive_property(schema)
        else:
            print(f"Unrecognized 'from_property' type {schema.type}")
            exit(1)

    def write_required_property(self, required_property: RequiredProperty) -> List[str]:
        res = []
        res.extend(
            [
                f'auto {required_property.variable_name}_val = yyjson_obj_get(obj, "{required_property.property_name}");',
                f'if (!{required_property.variable_name}_val) {{',
            ]
        )
        if required_property.default is not None:
            res.extend([f'\t{x}' for x in required_property.default])
        else:
            res.extend(
                [f"""\treturn "{self.name} required property '{required_property.property_name}' is missing";"""]
            )
        res.extend(['} else {'])
        res.extend([f'\t{x}' for x in required_property.body])
        res.append('}')
        return res

    def write_optional_property(self, optional_property: OptionalProperty) -> List[str]:
        res = []
        if optional_property.nullable:
            optionally_null = ''
        else:
            optionally_null = f' && !yyjson_is_null({optional_property.variable_name}_val)'
        res.extend(
            [
                f'auto {optional_property.variable_name}_val = yyjson_obj_get(obj, "{optional_property.property_name}");',
                f'if ({optional_property.variable_name}_val{optionally_null}) {{',
                f'\thas_{optional_property.variable_name} = true;',
            ]
        )
        res.extend([f'\t{x}' for x in optional_property.body])
        res.append('}')
        return res

    def write_additional_properties(self) -> List[str]:
        if not self.additional_properties:
            return []
        res = []

        res.extend(self.additional_properties.exclude_list)
        res.extend(
            [
                'size_t idx, max;',
                'yyjson_val *key, *val;',
                'yyjson_obj_foreach(obj, idx, max, key, val) {',
            ]
        )
        # FIXME: check for null in returned char*?
        res.append('\tauto key_str = yyjson_get_str(key);')
        res.extend(self.additional_properties.skip_if_excluded)
        res.extend(self.additional_properties.body)
        res.extend(
            [
                '\tadditional_properties.emplace(key_str, std::move(tmp));',
                '}',
            ]
        )
        return res

    def write_all_of(self) -> List[str]:
        if not self.all_of:
            return []
        res = []
        for item in self.all_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);' 'if (!error.empty()) {',
                    '\treturn error;',
                    '}',
                ]
            )
        return res

    def write_one_of(self) -> List[str]:
        if not self.one_of:
            return []
        res = []
        res.append('do {')
        for item in self.one_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);',
                    'if (error.empty()) {',
                    f'\thas_{item.name} = true;',
                    '\tbreak;',
                    '}',
                ]
            )
        res.append(f'\treturn "{self.name} failed to parse, none of the oneOf candidates matched";')
        res.append('} while (false);')
        return res

    def write_any_of(self) -> List[str]:
        if not self.any_of:
            return []
        res = []

        all_options = sorted([f'!has_{item.name}' for item in self.any_of])
        condition = ' && '.join(all_options)

        for item in self.any_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);',
                    'if (error.empty()) {',
                    f'\thas_{item.name} = true;',
                    '}',
                ]
            )

        res.extend(['if (' + condition + ') {', f'\treturn "{self.name} failed to parse, none of the anyOf candidates matched";', '}'])
        return res

    def write_nested_classes_header(self) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for nested_class in self.nested_classes.values():
            res.extend(nested_class.write_header())
            res.append('')
        return [f'\t{x}' if x else '' for x in res]

    def write_nested_classes_source(self, base_class: List[str]) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for nested_class in self.nested_classes.values():
            res.extend(nested_class.write_source(base_class + [self.name]))
        return res

    def write_variables(self) -> List[str]:
        if not self.variables:
            return []
        return ['public:'] + self.variables

    def collect_property_names(self, property: Property, visited: Optional[Set[str]] = None) -> Set[str]:
        if visited is None:
            visited = set()

        if property.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, property)
            if schema_property.ref in visited:
                return set()
            visited = set(visited)
            visited.add(schema_property.ref)
            return self.collect_property_names(self.parse_info.parsed_schemas[schema_property.ref], visited)

        if property.type != Property.Type.OBJECT:
            return set()

        object_property = cast(ObjectProperty, property)
        names = set(object_property.properties.keys())
        for base_property in object_property.all_of:
            names.update(self.collect_property_names(base_property, visited))
        return names

    def collect_all_of_property_names(self, property: Property) -> Set[str]:
        if not property.all_of:
            return set()

        seen: Set[str] = set()
        for base_property in property.all_of:
            base_names = self.collect_property_names(base_property)
            overlap = seen.intersection(base_names)
            if overlap:
                overlap_str = ', '.join(sorted(overlap))
                print(f"Schema '{self.name}' has duplicate allOf base properties: {overlap_str}")
                exit(1)
            seen.update(base_names)
        return seen

    def validate_polymorphic_property_ownership(self, property: Property, inherited_properties: Set[str]) -> None:
        local_properties = set(cast(ObjectProperty, property).properties.keys()) - inherited_properties

        for composition_name, variants in (('anyOf', property.any_of), ('oneOf', property.one_of)):
            for variant in variants:
                variant_properties = self.collect_property_names(variant)
                overlap = local_properties.intersection(variant_properties)
                if overlap:
                    overlap_str = ', '.join(sorted(overlap))
                    print(
                        f"Schema '{self.name}' has duplicate properties shared between local fields and {composition_name}: {overlap_str}"
                    )
                    exit(1)

    def direct_copy_expression(self, source: str, schema: Property) -> str:
        if schema.type == Property.Type.PRIMITIVE:
            return source
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            if object_property.is_raw_object():
                return source
            print(f"Unhandled object copy expression for '{source}'")
            exit(1)
        if schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in self.parse_info.recursive_schemas:
                return f'{source} ? make_uniq<{schema_property.ref}>({source}->Copy()) : nullptr'
            return f'{source}.Copy()'
        print(f"Unhandled direct copy expression type for '{source}': {schema.type}")
        exit(1)

    def write_copy_assignment_lines(self, target: str, source: str, schema: Optional[Property]) -> List[str]:
        if schema is None:
            return [f'{target} = {source};']
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            item_type = array_property.item_type
            item_copy = self.direct_copy_expression('item', item_type)
            return [
                f'{target}.reserve({source}.size());',
                f'for (auto &item : {source}) {{',
                f'\t{target}.emplace_back({item_copy});',
                '}',
            ]
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            if object_property.additional_properties:
                value_copy = self.direct_copy_expression('entry.second', object_property.additional_properties)
                return [
                    f'for (auto &entry : {source}) {{',
                    f'\t{target}.emplace(entry.first, {value_copy});',
                    '}',
                ]
        return [f'{target} = {self.direct_copy_expression(source, schema)};']

    def write_copy_method_source(self, base: str) -> List[str]:
        res = [
            '',
            f'{base}{self.name} {base}{self.name}::Copy() const {{',
            f'\t{self.name} res;',
        ]
        for member in self.members:
            lines = self.write_copy_assignment_lines(f'res.{member.variable_name}', member.variable_name, member.schema)
            if member.copy_guard is not None:
                lines = [f'if ({member.copy_guard}) {{'] + [f'\t{x}' for x in lines] + ['}']
            res.extend([f'\t{x}' for x in lines])
        res.extend(['\treturn res;', '}'])
        return res

    def write_source(self, base_class: List[str]) -> List[str]:
        res = []
        base = '::'.join(base_class) + '::' if base_class else ''
        qualified_name = f'{base}{self.name}'
        supports_population = self.supports_json_object_population()

        res.append(f'{qualified_name}::{self.name}() {{}}')
        res.extend(self.write_nested_classes_source(base_class))

        # Deserialization method
        res.extend(
            [
                '',
                f'{qualified_name} {qualified_name}::FromJSON(yyjson_val *obj) {{',
                f'\t{self.name} res;',
                '\tauto error = res.TryFromJSON(obj);',
                '\tif (!error.empty()) {',
                '\t\tthrow InvalidInputException(error);',
                '\t}',
                '\treturn res;',
                '}',
            ]
        )
        res.extend(self.write_copy_method_source(base))
        res.extend(
            [
                '',
                f'string {qualified_name}::TryFromJSON(yyjson_val *obj) {{',
                '\tstring error;',
            ]
        )
        res.extend([f'\t{x}' for x in self.write_all_of()])
        res.extend([f'\t{x}' for x in self.write_one_of()])
        res.extend([f'\t{x}' for x in self.write_any_of()])
        res.extend(self.try_from_json_body)
        res.extend(
            [
                '\treturn "";',
                '}',
                '',
            ]
        )

        if self.name not in SERIALIZATION_EXCLUDED:
            # Serialization methods
            if supports_population:
                res.extend([''])
                res.extend(self.generate_populate_json_method(qualified_name))
                res.extend([''])
            res.extend(self.generate_to_json_method(qualified_name))
        else:
            res.extend([
                f"yyjson_mut_val* {qualified_name}::ToJSON(yyjson_mut_doc *doc) const {{",
                f'''\tthrow InternalException("Can't serialize this class ({self.name})"); }}''',
            ])
        return res

    def write_header(self) -> List[str]:
        res = []
        supports_population = self.supports_json_object_population() and self.name not in SERIALIZATION_EXCLUDED
        res.extend(
            [
                f'class {self.name} {{',
                'public:',
                f'\t{self.name}();',
                f'\t{self.name}(const {self.name}&) = delete;',
                f'\t{self.name}& operator=(const {self.name}&) = delete;',
                f'\t{self.name}({self.name}&&) = default;',
                f'\t{self.name} &operator=({self.name}&&) = default;',
            ]
        )
        res.extend(self.write_nested_classes_header())
        res.extend(
            [
                'public:',
                '\t// Deserialization',
                f'\tstatic {self.name} FromJSON(yyjson_val *obj);',
                '\tstring TryFromJSON(yyjson_val *obj);',
                '',
                '\t// Copy',
                f'\t{self.name} Copy() const;',
                '',
                '\t// Serialization',
            ]
        )
        if supports_population:
            res.append('\tvoid PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;')
        res.extend([
            '\tyyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;',
            '',
        ])
        res.extend(self.write_variables())
        res.append('};')
        return res

    def generate_all_of(self, property: Property):
        if not property.all_of:
            return
        for item in property.all_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.all_of.append(AllOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.add_member(property_name, self.generate_variable_type(item), item)

    def generate_any_of(self, property: Property):
        if not property.any_of:
            return
        for item in property.any_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.any_of.append(AnyOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.add_member(property_name, self.generate_variable_type(item), item, copy_guard=f'has_{property_name}')
            self.add_member(f'has_{property_name}', 'bool', None, 'false')

    def generate_one_of(self, property: Property):
        if not property.one_of:
            return
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.one_of.append(OneOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.add_member(property_name, self.generate_variable_type(item), item, copy_guard=f'has_{property_name}')
            self.add_member(f'has_{property_name}', 'bool', None, 'false')

    def generate_array_loop(self, array_name, destination_name, array_property: ArrayProperty) -> List[str]:
        item_type = array_property.item_type
        body = []
        body.append('size_t idx, max;')
        body.append('yyjson_val *val;')
        body.append(f'yyjson_arr_foreach({array_name}, idx, max, val) {{')

        assignment = 'std::move(tmp)'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'{self.generate_variable_type(item_type)} tmp;')
            body.extend(self.generate_item_parse(item_type, 'val', 'tmp', True))
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            self.referenced_schemas.add(schema_property.ref)
            item_definition = ''
            if schema_property.ref in self.parse_info.recursive_schemas:
                body.extend([f'\tauto tmp_p = make_uniq<{schema_property.ref}>();', '\tauto &tmp = *tmp_p;'])
                assignment = 'std::move(tmp_p)'
            else:
                body.append(f'\t{schema_property.ref} tmp;')
            body.extend(['\terror = tmp.TryFromJSON(val);', '\tif (!error.empty()) {', '\t\treturn error;', '\t}'])
        body.append(f'\t{destination_name}.emplace_back({assignment});')
        body.append('}')

        res = []
        prefix = ''
        if array_property.nullable is not None:
            prefix = '} else '
            if array_property.nullable == True:
                res.extend(
                    [f'if (yyjson_is_null({array_name})) {{', '\t//! do nothing, property is explicitly nullable']
                )
            else:
                res.extend(
                    [
                        f'if (yyjson_is_null({array_name})) {{',
                        f'''\treturn "{self.name} property '{destination_name}' is not nullable, but is 'null'";''',
                    ]
                )

        res.append(f'{prefix}if (yyjson_is_arr({array_name})) {{')
        res.extend([f'\t{x}' for x in body])
        res.extend(
            [
                '} else {',
                f"""\treturn StringUtil::Format("{self.name} property '{destination_name}' is not of type 'array', found '%s' instead", yyjson_get_type_desc({array_name}));""",
                '}',
            ]
        )

        return res

    def generate_item_parse(self, property: Property, source: str, target: str, is_required: bool) -> List[str]:
        res = []
        prefix = ''
        if property.nullable is not None:
            prefix = '} else '
            if property.nullable == True:
                res.extend(
                    [
                        f'if (yyjson_is_null({source})) {{',
                        '\t//! do nothing, property is explicitly nullable',
                    ]
                )
                if not is_required:
                    res.extend([f'\thas_{target} = false;'])
            else:
                res.extend(
                    [
                        f'if (yyjson_is_null({source})) {{',
                        f'''\treturn "{self.name} property '{target}' is not nullable, but is 'null'";''',
                    ]
                )

        if property.type == Property.Type.SCHEMA_REFERENCE:
            print(f"Unrecognized property type {property.type}, {source}")
            exit(1)
        if property.type == Property.Type.ARRAY:
            # TODO: maybe we move the array parse to a function that creates a vector<...>, instead of parsing it inline
            print(f'Nested arrays are not supported, hopefully we dont have to!')
            exit(1)
        elif property.type == Property.Type.PRIMITIVE:
            # FIXME: add a check to see that the yyjson_val* is of the right type
            # FIXME: check for null in returned char* for 'yyjson_get_str?
            primitive_property = cast(PrimitiveProperty, property)
            item_type = primitive_property.primitive_type
            if item_type not in PRIMITIVE_TYPE_MAPPING:
                print(f"Primitive type '{item_type}' not in PRIMITIVE_TYPE_MAPPING")
                exit(1)

            type_mapping: PrimitiveTypeMapping = PRIMITIVE_TYPE_MAPPING[item_type]
            specific_mapping = None
            generic_mapping = None
            if type_mapping.formats and property.format in type_mapping.formats:
                assert item_type == 'integer'
                specific_mapping = type_mapping.formats[property.format]
            generic_mapping = type_mapping
            # NOTE: no need to really check the 'format' of the 'property' here
            # FIXME: 'target' is not the property name in the spec, it's already been transformed to the cpp variable name
            if specific_mapping:
                res.extend(
                    [
                        f'{prefix}if ({specific_mapping.type_check}({source})) {{',
                        f'\t{target} = {specific_mapping.conversion}({source});',
                    ]
                )
                res.extend(
                    [
                        f'}} else if (yyjson_is_uint({source})) {{',
                        f'\t{target} = yyjson_get_uint({source});',
                    ]
                )
            else:
                res.extend(
                    [
                        f'{prefix}if ({generic_mapping.type_check}({source})) {{',
                        f'\t{target} = {generic_mapping.conversion}({source});',
                    ]
                )

            res.extend(
                [
                    '} else {',
                    f"""\treturn StringUtil::Format("{self.name} property '{target}' is not of type '{item_type}', found '%s' instead", yyjson_get_type_desc({source}));""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.is_raw_object():
            res.extend(
                [
                    f'{prefix}if (yyjson_is_obj({source})) {{',
                    f'\t{target} = {source};',
                    '} else {',
                    f"""\treturn "{self.name} property '{target}' is not of type 'object'";""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.additional_properties:
            object_property = cast(ObjectProperty, property)
            additional_properties = property.additional_properties

            res.append(f'{prefix}if (yyjson_is_obj({source})) {{')
            res.extend(
                [
                    '\tsize_t idx, max;',
                    '\tyyjson_val *key, *val;',
                    f'\tyyjson_obj_foreach({source}, idx, max, key, val) {{',
                ]
            )
            # FIXME: check for null in returned char*?
            res.append('\t\tauto key_str = yyjson_get_str(key);')
            res.append(f'\t\t{self.generate_variable_type(additional_properties)} tmp;')

            if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
                item_definition = [
                    f'\t\t{x}' for x in self.generate_item_parse(additional_properties, 'val', 'tmp', True)
                ]
                res.extend(item_definition)
            else:
                schema_property = cast(SchemaReferenceProperty, additional_properties)
                self.referenced_schemas.add(schema_property.ref)
                if schema_property.ref in self.parse_info.recursive_schemas:
                    print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                    exit(1)
                res.append(f'\t\t{schema_property.ref} tmp;')
                res.extend(
                    [
                        '\t\terror = tmp.TryFromJSON(val);',
                        '\t\tif (!error.empty()) {',
                        '\t\t\treturn error;',
                        '\t\t}',
                    ]
                )
            res.extend(
                [
                    f'\t\t{target}.emplace(key_str, std::move(tmp));',
                    '\t}',
                ]
            )
            res.extend(['} else {', f"""\treturn "{self.name} property '{target}' is not of type 'object'";""", '}'])
        else:
            print(f"Unrecognized type in 'generate_item_parse', {property.type}")
            exit(1)
        return res

    def generate_assignment(self, schema: Property, target: str, source: str, is_required: bool) -> List[str]:
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            return self.generate_array_loop(source, target, array_property)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            self.referenced_schemas.add(schema_property.ref)
            result = []
            dereference_style = '.'
            if schema_property.ref in self.parse_info.recursive_schemas:
                result.append(f'{target} = make_uniq<{schema_property.ref}>();')
                dereference_style = '->'
            result.extend(
                [
                    f'error = {target}{dereference_style}TryFromJSON({source});',
                    'if (!error.empty()) {',
                    '    return error;',
                    '}',
                ]
            )
            return result
        else:
            return self.generate_item_parse(schema, source, target, is_required)

    def generate_optional_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, optional_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(optional_property, variable_name, f'{variable_name}_val', False)
            self.optional_properties[item] = OptionalProperty(
                property_name=item,
                variable_name=variable_name,
                body=body,
                schema=optional_property,
                nullable=optional_property.nullable,
            )
            variable_type = self.generate_variable_type(optional_property)
            self.add_member(variable_name, variable_type, optional_property, copy_guard=f'has_{variable_name}')
            self.add_member(f'has_{variable_name}', 'bool', None, 'false')

    def generate_required_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(required_property, variable_name, f'{variable_name}_val', True)
            if required_property.default is not None:
                default = [f'{variable_name} = "{str(required_property.default)}";']
            else:
                default = None
            self.required_properties[item] = RequiredProperty(
                property_name=item, variable_name=variable_name, body=body, default=default, schema=required_property
            )
            variable_type = self.generate_variable_type(required_property)
            self.add_member(variable_name, variable_type, required_property)

    def generate_additional_properties(self, properties: List[str], additional_properties: Property):
        if not additional_properties:
            return

        skip_if_excluded = []
        exclude_list = []
        if properties:
            exclude_list = [
                'case_insensitive_set_t handled_properties {',
                f"""\t\t{', '.join(f'"{x}"' for x in properties)} }};""",
            ]
            skip_if_excluded = [
                '\tif (handled_properties.count(key_str)) {',
                '\t\tcontinue;',
                '\t}',
            ]

        body = []
        if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'\t{self.generate_variable_type(additional_properties)} tmp;')
            body.extend(self.generate_item_parse(additional_properties, 'val', 'tmp', True))
        else:
            schema_property = cast(SchemaReferenceProperty, additional_properties)
            self.referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.parse_info.recursive_schemas:
                print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                exit(1)
            body.append(f'\t{schema_property.ref} tmp;')
            body.extend(
                [
                    'error = tmp.TryFromJSON(val);',
                    'if (!error.empty()) {',
                    '\treturn error;',
                    '}',
                ]
            )
        self.additional_properties = AdditionalProperty(
            body=body, exclude_list=exclude_list, skip_if_excluded=skip_if_excluded, schema=additional_properties
        )
        variable_type = self.generate_variable_type(additional_properties)
        member_schema = ObjectProperty()
        member_schema.additional_properties = additional_properties
        self.add_member('additional_properties', f'case_insensitive_map_t<{variable_type}>', member_schema)

    def generate_variable_type(self, schema: Property) -> str:
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            assert not object_property.properties
            if object_property.additional_properties:
                variable_type = self.generate_variable_type(object_property.additional_properties)
                return f'case_insensitive_map_t<{variable_type}>'
            return 'yyjson_val *'
        elif schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            item_type = self.generate_variable_type(array_property.item_type)
            return f'vector<{item_type}>'
        elif schema.type == Property.Type.PRIMITIVE:
            primitive_property = cast(PrimitiveProperty, schema)
            primitive_type = primitive_property.primitive_type
            if primitive_type in PRIMITIVE_TYPE_MAPPING:
                mapping = PRIMITIVE_TYPE_MAPPING[primitive_type]
                if mapping.formats and schema.format in mapping.formats:
                    return mapping.formats[schema.format].cpp_type
                return PRIMITIVE_TYPE_MAPPING[primitive_type].cpp_type
            elif primitive_type == 'number':
                if not primitive_property.format:
                    print(f"'number' without a 'format' property in the spec!")
                    exit(1)
                return primitive_property.format
            else:
                print(f"Unrecognized primitive type '{primitive_type}' in 'generate_variable_type'")
                exit(1)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in self.parse_info.recursive_schemas:
                return f'unique_ptr<{schema_property.ref}>'
            return schema_property.ref
        else:
            print(f"Unrecognized 'generate_variable_type' type {schema.type}")
            exit(1)

    def generate_nested_class_definitions(self):
        generated_schemas_referenced = [x for x in self.referenced_schemas if x not in self.parse_info.schemas]
        for item in generated_schemas_referenced:
            parsed_schema = self.parse_info.parsed_schemas[item]
            nested_class = CPPClass(item, self.parse_info)
            nested_class.from_property(parsed_schema)
            self.nested_classes[item] = nested_class

    def schema_supports_json_object_population(
        self, schema: Optional[Property], visited: Optional[Set[str]] = None
    ) -> bool:
        if schema is None:
            return False

        if visited is None:
            visited = set()

        if schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in visited:
                return True
            next_visited = set(visited)
            next_visited.add(schema_property.ref)
            return self.schema_supports_json_object_population(
                self.parse_info.parsed_schemas[schema_property.ref], next_visited
            )

        if schema.type != Property.Type.OBJECT:
            return False

        object_schema = cast(ObjectProperty, schema)
        has_object_content = (
            bool(object_schema.all_of)
            or bool(object_schema.properties)
            or object_schema.additional_properties is not None
        )

        if object_schema.one_of:
            return all(self.schema_supports_json_object_population(item, visited) for item in object_schema.one_of)
        if object_schema.any_of and not has_object_content:
            return all(self.schema_supports_json_object_population(item, visited) for item in object_schema.any_of)
        return True

    def supports_json_object_population(self) -> bool:
        return self.schema_supports_json_object_population(self.parse_info.parsed_schemas.get(self.name))

    def class_supports_json_object_population(self, class_name: str) -> bool:
        return class_name not in SERIALIZATION_EXCLUDED and self.schema_supports_json_object_population(
            self.parse_info.parsed_schemas[class_name]
        )

    def _generate_json_object_merge(self, source_expr: str, temp_name: str, indent: int = 1) -> List[str]:
        prefix = '\t' * indent
        return [
            f'{prefix}yyjson_mut_val *{temp_name} = {source_expr};',
            f'{prefix}if (!yyjson_mut_is_obj({temp_name})) {{',
            f'{prefix}\tthrow InternalException("PopulateJSON requires an object-like JSON value");',
            f'{prefix}}}',
            f'{prefix}{{',
            f'{prefix}\tsize_t idx, max;',
            f'{prefix}\tyyjson_mut_val *key, *val;',
            f'{prefix}\tyyjson_mut_obj_foreach({temp_name}, idx, max, key, val) {{',
            f'{prefix}\t\tyyjson_mut_obj_add(obj, key, val);',
            f'{prefix}\t}}',
            f'{prefix}}}',
        ]

    def generate_populate_json_method(self, qualified_name: str) -> List[str]:
        lines = [
            f"void {qualified_name}::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {{",
            "\tif (!yyjson_mut_is_obj(obj)) {",
            '\t\tthrow InternalException("PopulateJSON requires obj to be a JSON object");',
            "\t}",
            "",
        ]

        if self.one_of:
            for i, variant in enumerate(self.one_of):
                if i == 0:
                    lines.append(f"\tif (has_{variant.name}) {{")
                else:
                    lines.append(f"\t}} else if (has_{variant.name}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    if variant.dereference_style == '->':
                        lines.append(f"\t\t{variant.name}->PopulateJSON(doc, obj);")
                    else:
                        lines.append(f"\t\t{variant.name}.PopulateJSON(doc, obj);")
                else:
                    accessor = f"{variant.name}{variant.dereference_style}ToJSON(doc)"
                    lines.extend(self._generate_json_object_merge(accessor, f"{variant.name}_obj", indent=2))

            lines.extend([
                "\t}",
                "}",
            ])
            return lines

        any_of_has_properties = (
            self.all_of
            or self.required_properties
            or self.optional_properties
            or (self.additional_properties and self.additional_properties.schema)
        )

        if self.any_of and not any_of_has_properties:
            for i, variant in enumerate(self.any_of):
                if i == 0:
                    lines.append(f"\tif (has_{variant.name}) {{")
                else:
                    lines.append(f"\t}} else if (has_{variant.name}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    if variant.dereference_style == '->':
                        lines.append(f"\t\t{variant.name}->PopulateJSON(doc, obj);")
                    else:
                        lines.append(f"\t\t{variant.name}.PopulateJSON(doc, obj);")
                else:
                    accessor = f"{variant.name}{variant.dereference_style}ToJSON(doc)"
                    lines.extend(self._generate_json_object_merge(accessor, f"{variant.name}_obj", indent=2))

            lines.extend([
                "\t}",
                "}",
            ])
            return lines

        if self.any_of:
            for i, variant in enumerate(self.any_of):
                if i == 0:
                    lines.append(f"\tif (has_{variant.name}) {{")
                else:
                    lines.append(f"\t}} else if (has_{variant.name}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    if variant.dereference_style == '->':
                        lines.append(f"\t\t{variant.name}->PopulateJSON(doc, obj);")
                    else:
                        lines.append(f"\t\t{variant.name}.PopulateJSON(doc, obj);")
                else:
                    accessor = f"{variant.name}{variant.dereference_style}ToJSON(doc)"
                    lines.extend(self._generate_json_object_merge(accessor, f"{variant.name}_obj", indent=2))

            lines.append("\t}")
            lines.append("")

        if self.all_of:
            for base in self.all_of:
                if base.class_name:
                    lines.append(f"\t// Serialize base class: {base.class_name}")
                    if self.class_supports_json_object_population(base.class_name):
                        lines.append(f"\t{base.name}.PopulateJSON(doc, obj);")
                    else:
                        lines.extend(
                            self._generate_json_object_merge(
                                f"{base.name}.ToJSON(doc)", f"{base.name}base_obj", indent=1
                            )
                        )
                    lines.append("")

        for _, prop in self.required_properties.items():
            lines.extend(
                self._generate_property_serialization(
                    prop.variable_name,
                    prop.property_name,
                    prop.schema,
                    required=True,
                )
            )

        for _, prop in self.optional_properties.items():
            lines.extend(
                self._generate_property_serialization(
                    prop.variable_name,
                    prop.property_name,
                    prop.schema,
                    required=False,
                )
            )

        if self.additional_properties and self.additional_properties.schema:
            lines.extend(self._generate_additional_properties_serialization())

        lines.append("}")
        return lines

    # ==================== SERIALIZATION METHODS ====================

    def generate_to_json_method(self, qualified_name: str) -> List[str]:
        """Generate ToJSON method implementation"""

        root_schema = self.parse_info.parsed_schemas.get(self.name)
        supports_population = self.supports_json_object_population()

        if root_schema and root_schema.type == Property.Type.PRIMITIVE:
            prim = cast(PrimitiveProperty, root_schema)
            prim_type = prim.primitive_type

            lines = [
                f"yyjson_mut_val* {qualified_name}::ToJSON(yyjson_mut_doc *doc) const {{"
            ]

            if prim_type == 'string':
                lines.append("\treturn yyjson_mut_strcpy(doc, value.c_str());")
            elif prim_type == 'integer':
                if prim.format == 'int64':
                    lines.append("\treturn yyjson_mut_sint(doc, value);")
                else:
                    lines.append("\treturn yyjson_mut_int(doc, value);")
            elif prim_type == 'boolean':
                lines.append("\treturn yyjson_mut_bool(doc, value);")
            elif prim_type == 'number':
                lines.append("\treturn yyjson_mut_real(doc, value);")
            else:
                lines.append('\tthrow InternalException("Unsupported primitive serialization");')

            lines.append("}")
            return lines

        if root_schema and root_schema.type == Property.Type.ARRAY:
            array_schema = cast(ArrayProperty, root_schema)
            lines = [
                f"yyjson_mut_val* {qualified_name}::ToJSON(yyjson_mut_doc *doc) const {{",
                "\tyyjson_mut_val *arr = yyjson_mut_arr(doc);",
                "\tfor (const auto &item : value) {"
            ]

            item_type = array_schema.item_type
            if item_type.type == Property.Type.PRIMITIVE:
                prim_item = cast(PrimitiveProperty, item_type)
                if prim_item.primitive_type == 'string':
                    lines.append("\t\tyyjson_mut_arr_append(arr, yyjson_mut_str(doc, item.c_str()));")
                elif prim_item.primitive_type == 'integer':
                    if prim_item.format == 'int64':
                        lines.append("\t\tyyjson_mut_arr_append(arr, yyjson_mut_sint(doc, item));")
                    else:
                        lines.append("\t\tyyjson_mut_arr_append(arr, yyjson_mut_int(doc, item));")
                elif prim_item.primitive_type == 'boolean':
                    lines.append("\t\tyyjson_mut_arr_append(arr, yyjson_mut_bool(doc, item));")
                elif prim_item.primitive_type == 'number':
                    lines.append("\t\tyyjson_mut_arr_append(arr, yyjson_mut_real(doc, item));")
            elif item_type.type == Property.Type.SCHEMA_REFERENCE:
                schema_ref = cast(SchemaReferenceProperty, item_type)
                if schema_ref.ref in self.parse_info.recursive_schemas:
                    lines.append("\t\tyyjson_mut_arr_append(arr, item->ToJSON(doc));")
                else:
                    lines.append("\t\tyyjson_mut_arr_append(arr, item.ToJSON(doc));")

            lines.extend([
                "\t}",
                "\treturn arr;",
                "}"
            ])
            return lines

        if supports_population:
            return [
                f"yyjson_mut_val* {qualified_name}::ToJSON(yyjson_mut_doc *doc) const {{",
                "\tyyjson_mut_val *obj = yyjson_mut_obj(doc);",
                "\tPopulateJSON(doc, obj);",
                "\treturn obj;",
                "}",
            ]

        lines = []
        lines.extend([
            f"yyjson_mut_val* {qualified_name}::ToJSON(yyjson_mut_doc *doc) const {{",
        ])

        if self.one_of:
            for i, variant in enumerate(self.one_of):
                if i == 0:
                    lines.append(f"\tif (has_{variant.name}) {{")
                else:
                    lines.append(f"\t}} else if (has_{variant.name}) {{")

                if variant.dereference_style == '->':
                    lines.append(f"\t\treturn {variant.name}->ToJSON(doc);")
                else:
                    lines.append(f"\t\treturn {variant.name}.ToJSON(doc);")

            lines.extend([
                "\t}",
                "\t// No variant is active - return empty object",
                "\treturn yyjson_mut_obj(doc);",
                "}"
            ])
            return lines

        any_of_has_properties = (
            self.all_of
            or self.required_properties
            or self.optional_properties
            or (self.additional_properties and self.additional_properties.schema)
        )
        any_of_is_primitive = self.any_of and all(
            self.parse_info.parsed_schemas[variant.class_name].type == Property.Type.PRIMITIVE
            for variant in self.any_of
        )
        serialization_any_of = self.any_of
        if any_of_is_primitive:
            def primitive_variant_priority(variant: AnyOf) -> int:
                schema = cast(PrimitiveProperty, self.parse_info.parsed_schemas[variant.class_name])
                if schema.primitive_type == 'integer':
                    return 2 if schema.format == 'int64' else 1
                if schema.primitive_type == 'number':
                    return 2 if schema.format == 'double' else 1
                return 0

            serialization_any_of = sorted(self.any_of, key=primitive_variant_priority, reverse=True)

        if self.any_of and not any_of_has_properties:
            for i, variant in enumerate(serialization_any_of):
                if i == 0:
                    lines.append(f"\tif (has_{variant.name}) {{")
                else:
                    lines.append(f"\t}} else if (has_{variant.name}) {{")

                if variant.dereference_style == '->':
                    lines.append(f"\t\treturn {variant.name}->ToJSON(doc);")
                else:
                    lines.append(f"\t\treturn {variant.name}.ToJSON(doc);")

            lines.extend([
                "\t}",
                "\t// No variant is active - return null"
                if any_of_is_primitive
                else "\t// No variant is active - return empty object",
                "\treturn yyjson_mut_null(doc);" if any_of_is_primitive else "\treturn yyjson_mut_obj(doc);",
                "}"
            ])
            return lines

        lines.extend([
            '\tthrow InternalException("ToJSON should use PopulateJSON for object-like schemas");',
            "}",
        ])
        return lines

    def _generate_property_serialization(
        self,
        var_name: str,
        json_name: str,
        property_schema: Property,
        required: bool
    ) -> List[str]:
        """Generate serialization code for a single property"""
        
        lines = []
        
        # Comment
        lines.append(f"\t// Serialize: {json_name}")
        
        if not required:
            # Wrap optional properties in has_XXX check
            lines.append(f"\tif (has_{var_name}) {{")
            inner_lines = self._serialize_value(
                var_name, json_name, property_schema, indent=2
            )
            lines.extend(inner_lines)
            lines.append("\t}")
        else:
            lines.extend(
                self._serialize_value(
                    var_name, json_name, property_schema, indent=1
                )
            )
        
        lines.append("")
        return lines

    def _serialize_value(
        self,
        var_name: str,
        json_name: str,
        property_schema: Property,
        indent: int
    ) -> List[str]:
        """Generate serialization code based on property type"""
        
        prefix = '\t' * indent
        
        if property_schema.type == Property.Type.PRIMITIVE:
            return self._serialize_primitive(
                var_name, json_name, 
                cast(PrimitiveProperty, property_schema), 
                prefix
            )
        elif property_schema.type == Property.Type.ARRAY:
            return self._serialize_array(
                var_name, json_name,
                cast(ArrayProperty, property_schema),
                prefix
            )
        elif property_schema.type == Property.Type.SCHEMA_REFERENCE:
            return self._serialize_schema_reference(
                var_name, json_name,
                cast(SchemaReferenceProperty, property_schema),
                prefix
            )
        elif property_schema.type == Property.Type.OBJECT:
            return self._serialize_object(
                var_name, json_name,
                cast(ObjectProperty, property_schema),
                prefix
            )
        
        return [f"{prefix}// TODO: Unknown type for {var_name}"]

    def _serialize_primitive(
        self,
        var_name: str,
        json_name: str,
        prop: PrimitiveProperty,
        prefix: str
    ) -> List[str]:
        """Serialize primitive types"""
        
        prim_type = prop.primitive_type
        
        if prim_type == 'string':
            return [
                f'{prefix}yyjson_mut_obj_add_str(doc, obj, "{json_name}", {var_name}.c_str());'
            ]
        elif prim_type == 'integer':
            if prop.format == 'int64':
                return [
                    f'{prefix}yyjson_mut_obj_add_sint(doc, obj, "{json_name}", {var_name});'
                ]
            else:
                return [
                    f'{prefix}yyjson_mut_obj_add_int(doc, obj, "{json_name}", {var_name});'
                ]
        elif prim_type == 'boolean':
            return [
                f'{prefix}yyjson_mut_obj_add_bool(doc, obj, "{json_name}", {var_name});'
            ]
        elif prim_type == 'number':
            return [
                f'{prefix}yyjson_mut_obj_add_real(doc, obj, "{json_name}", {var_name});'
            ]
        else:
            return [
                f'{prefix}// TODO: Unsupported primitive type: {prim_type}'
            ]

    def _serialize_array(
        self,
        var_name: str,
        json_name: str,
        prop: ArrayProperty,
        prefix: str
    ) -> List[str]:
        """Serialize array types"""
        
        lines = [
            f'{prefix}yyjson_mut_val *{var_name}_arr = yyjson_mut_arr(doc);',
            f'{prefix}for (const auto &item : {var_name}) {{'
        ]
        
        # Generate item serialization based on item type
        item_type = prop.item_type
        
        if item_type.type == Property.Type.PRIMITIVE:
            prim_item = cast(PrimitiveProperty, item_type)
            lines.extend(
                self._serialize_array_primitive_item(prim_item, prefix)
            )
        elif item_type.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, item_type)
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.append(
                    f'{prefix}\tyyjson_mut_val *item_val = item->ToJSON(doc);'
                )
            else:
                lines.append(
                    f'{prefix}\tyyjson_mut_val *item_val = item.ToJSON(doc);'
                )
        elif item_type.type == Property.Type.OBJECT:
            # Object/Map array items
            object_item = cast(ObjectProperty, item_type)
            object_item_serialization = self._serialize_array_object_item(object_item, prefix)
            if not object_item_serialization:
                lines.extend([
                    f'''{prefix}\tthrow InvalidInputException("Can't serialize this object");''',
                    f'{prefix}}}',
                ])
                return lines
            else:
                lines.extend(object_item_serialization)
        elif item_type.type == Property.Type.ARRAY:
            # Nested arrays (array of arrays)
            nested_array = cast(ArrayProperty, item_type)
            lines.extend(
                self._serialize_nested_array_item(nested_array, prefix)
            )
        
        lines.extend([
            f'{prefix}\tyyjson_mut_arr_append({var_name}_arr, item_val);',
            f'{prefix}}}',
            f'{prefix}yyjson_mut_obj_add_val(doc, obj, "{json_name}", {var_name}_arr);'
        ])
        
        return lines

    def _serialize_array_primitive_item(
        self, 
        prim_prop: PrimitiveProperty, 
        prefix: str
    ) -> List[str]:
        """Serialize primitive array items"""
        
        prim_type = prim_prop.primitive_type
        
        if prim_type == 'string':
            return [
                f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());'
            ]
        elif prim_type == 'integer':
            if prim_prop.format == 'int64':
                return [
                    f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_sint(doc, item);'
                ]
            else:
                return [
                    f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_int(doc, item);'
                ]
        elif prim_type == 'boolean':
            return [
                f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_bool(doc, item);'
            ]
        elif prim_type == 'number':
            return [
                f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_real(doc, item);'
            ]
        else:
            return [
                f'{prefix}\t// TODO: Unsupported array item type: {prim_type}'
            ]

    def _serialize_array_object_item(
        self,
        object_prop: ObjectProperty,
        prefix: str
    ) -> Optional[List[str]]:
        """Serialize object/map array items"""
        
        lines = []
        
        # Case 1: Raw object (no properties, no additionalProperties)
        if object_prop.is_raw_object():
            return None
        
        # Case 2: Map/dictionary with additional properties
        if object_prop.additional_properties:
            lines.extend([
                f'{prefix}\t// Map object - serialize key-value pairs',
                f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_obj(doc);'
            ])
            
            value_type = object_prop.additional_properties
            
            if value_type.type == Property.Type.PRIMITIVE:
                lines.extend(
                    self._serialize_map_primitive_values(value_type, prefix + '\t')
                )
            elif value_type.type == Property.Type.SCHEMA_REFERENCE:
                lines.extend(
                    self._serialize_map_schema_ref_values(value_type, prefix + '\t')
                )
            elif value_type.type == Property.Type.ARRAY:
                lines.extend(
                    self._serialize_map_array_values(value_type, prefix + '\t')
                )
            elif value_type.type == Property.Type.OBJECT:
                lines.extend(
                    self._serialize_map_object_values(value_type, prefix + '\t')
                )
            
            return ('', lines)
        
        # Case 3: Object with defined properties
        if object_prop.properties:
            lines.extend([
                f'{prefix}\t// Object with properties - serialize each field',
                f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_obj(doc);'
            ])
            
            for prop_name, prop_schema in object_prop.properties.items():
                lines.extend(
                    self._serialize_inline_object_property(
                        prop_name, prop_schema, prefix + '\t'
                    )
                )
            
            return lines
        
        # Fallback
        lines.extend([
            f'{prefix}\t// Empty object',
            f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_obj(doc);'
        ])
        return lines

    def _serialize_map_primitive_values(
        self,
        prim_prop: PrimitiveProperty,
        prefix: str
    ) -> List[str]:
        """Serialize map with primitive values"""
        
        lines = [
            f'{prefix}for (const auto &it : item) {{',
            f'{prefix}\tauto &key = it.first;',
            f'{prefix}\tauto &value = it.second;',
        ]
        
        prim_type = prim_prop.primitive_type
        
        if prim_type == 'string':
            lines.append(
                f'{prefix}\tyyjson_mut_obj_add_str(doc, item_val, key.c_str(), value.c_str());'
            )
        elif prim_type == 'integer':
            if prim_prop.format == 'int64':
                lines.append(
                    f'{prefix}\tyyjson_mut_obj_add_sint(doc, item_val, key.c_str(), value);'
                )
            else:
                lines.append(
                    f'{prefix}\tyyjson_mut_obj_add_int(doc, item_val, key.c_str(), value);'
                )
        elif prim_type == 'boolean':
            lines.append(
                f'{prefix}\tyyjson_mut_obj_add_bool(doc, item_val, key.c_str(), value);'
            )
        elif prim_type == 'number':
            lines.append(
                f'{prefix}\tyyjson_mut_obj_add_real(doc, item_val, key.c_str(), value);'
            )
        
        lines.append(f'{prefix}}}')
        return lines

    def _serialize_map_schema_ref_values(
        self,
        schema_ref: SchemaReferenceProperty,
        prefix: str
    ) -> List[str]:
        """Serialize map with schema reference values"""
        
        lines = [
            f'{prefix}for (const auto &it : item) {{',
            f'{prefix}\tauto &key = it.first;',
            f'{prefix}\tauto &value = it.second;',
        ]
        
        if schema_ref.ref in self.parse_info.recursive_schemas:
            lines.extend([
                f'{prefix}\tyyjson_mut_val *value_obj = value->ToJSON(doc);',
                f'{prefix}\tyyjson_mut_obj_add_val(doc, item_val, key.c_str(), value_obj);'
            ])
        else:
            lines.extend([
                f'{prefix}\tyyjson_mut_val *value_obj = value.ToJSON(doc);',
                f'{prefix}\tyyjson_mut_obj_add_val(doc, item_val, key.c_str(), value_obj);'
            ])
        
        lines.append(f'{prefix}}}')
        return lines

    def _serialize_map_array_values(
        self,
        array_prop: ArrayProperty,
        prefix: str
    ) -> List[str]:
        """Serialize map with array values"""
        
        lines = [
            f'{prefix}for (const auto &[key, value_array] : item) {{',
            f'{prefix}\tyyjson_mut_val *value_arr = yyjson_mut_arr(doc);'
        ]
        
        item_type = array_prop.item_type
        
        if item_type.type == Property.Type.PRIMITIVE:
            prim_item = cast(PrimitiveProperty, item_type)
            lines.append(f'{prefix}\tfor (const auto &arr_item : value_array) {{')
            
            prim_type = prim_item.primitive_type
            if prim_type == 'string':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *arr_item_val = yyjson_mut_str(doc, arr_item.c_str());'
                )
            elif prim_type == 'integer':
                if prim_item.format == 'int64':
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *arr_item_val = yyjson_mut_sint(doc, arr_item);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *arr_item_val = yyjson_mut_int(doc, arr_item);'
                    )
            elif prim_type == 'boolean':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *arr_item_val = yyjson_mut_bool(doc, arr_item);'
                )
            elif prim_type == 'number':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *arr_item_val = yyjson_mut_real(doc, arr_item);'
                )
            
            lines.extend([
                f'{prefix}\t\tyyjson_mut_arr_append(value_arr, arr_item_val);',
                f'{prefix}\t}}'
            ])
            
        elif item_type.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, item_type)
            lines.append(f'{prefix}\tfor (const auto &arr_item : value_array) {{')
            
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *arr_item_val = arr_item->ToJSON(doc);'
                )
            else:
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *arr_item_val = arr_item.ToJSON(doc);'
                )
            
            lines.extend([
                f'{prefix}\t\tyyjson_mut_arr_append(value_arr, arr_item_val);',
                f'{prefix}\t}}'
            ])
        
        lines.extend([
            f'{prefix}\tyyjson_mut_obj_add_val(doc, item_val, key.c_str(), value_arr);',
            f'{prefix}}}'
        ])
        
        return lines

    def _serialize_map_object_values(
        self,
        object_prop: ObjectProperty,
        prefix: str
    ) -> List[str]:
        """Serialize map with object/map values (nested maps)"""
        
        lines = [
            f'{prefix}for (const auto &[key, value_map] : item) {{'
        ]
        
        if object_prop.is_raw_object():
            lines.extend([
                f'{prefix}\tyyjson_mut_val *value_obj = yyjson_mut_val_mut_copy(doc, value_map);',
                f'{prefix}\tyyjson_mut_obj_add_val(doc, item_val, key.c_str(), value_obj);'
            ])
        elif object_prop.additional_properties:
            lines.append(
                f'{prefix}\tyyjson_mut_val *value_obj = yyjson_mut_obj(doc);'
            )
            
            nested_value_type = object_prop.additional_properties
            
            if nested_value_type.type == Property.Type.PRIMITIVE:
                nested_prim = cast(PrimitiveProperty, nested_value_type)
                lines.append(
                    f'{prefix}\tfor (const auto &[nested_key, nested_value] : value_map) {{'
                )
                
                if nested_prim.primitive_type == 'string':
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_obj_add_str(doc, value_obj, nested_key.c_str(), nested_value.c_str());'
                    )
                elif nested_prim.primitive_type == 'integer':
                    if nested_prim.format == 'int64':
                        lines.append(
                            f'{prefix}\t\tyyjson_mut_obj_add_sint(doc, value_obj, nested_key.c_str(), nested_value);'
                        )
                    else:
                        lines.append(
                            f'{prefix}\t\tyyjson_mut_obj_add_int(doc, value_obj, nested_key.c_str(), nested_value);'
                        )
                elif nested_prim.primitive_type == 'boolean':
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_obj_add_bool(doc, value_obj, nested_key.c_str(), nested_value);'
                    )
                elif nested_prim.primitive_type == 'number':
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_obj_add_real(doc, value_obj, nested_key.c_str(), nested_value);'
                    )
                
                lines.append(f'{prefix}\t}}')
            
            elif nested_value_type.type == Property.Type.SCHEMA_REFERENCE:
                nested_ref = cast(SchemaReferenceProperty, nested_value_type)
                lines.append(
                    f'{prefix}\tfor (const auto &[nested_key, nested_value] : value_map) {{'
                )
                
                if nested_ref.ref in self.parse_info.recursive_schemas:
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *nested_obj = nested_value->ToJSON(doc);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *nested_obj = nested_value.ToJSON(doc);'
                    )
                
                lines.extend([
                    f'{prefix}\t\tyyjson_mut_obj_add_val(doc, value_obj, nested_key.c_str(), nested_obj);',
                    f'{prefix}\t}}'
                ])
            
            lines.append(
                f'{prefix}\tyyjson_mut_obj_add_val(doc, item_val, key.c_str(), value_obj);'
            )
        
        lines.append(f'{prefix}}}')
        return lines

    def _serialize_inline_object_property(
        self,
        prop_name: str,
        prop_schema: Property,
        prefix: str
    ) -> List[str]:
        """Serialize a property of an inline object"""
        
        lines = []
        
        if prop_schema.type == Property.Type.PRIMITIVE:
            prim_prop = cast(PrimitiveProperty, prop_schema)
            prim_type = prim_prop.primitive_type
            
            if prim_type == 'string':
                lines.append(
                    f'{prefix}yyjson_mut_obj_add_str(doc, item_val, "{prop_name}", item.{prop_name}.c_str());'
                )
            elif prim_type == 'integer':
                if prim_prop.format == 'int64':
                    lines.append(
                        f'{prefix}yyjson_mut_obj_add_sint(doc, item_val, "{prop_name}", item.{prop_name});'
                    )
                else:
                    lines.append(
                        f'{prefix}yyjson_mut_obj_add_int(doc, item_val, "{prop_name}", item.{prop_name});'
                    )
            elif prim_type == 'boolean':
                lines.append(
                    f'{prefix}yyjson_mut_obj_add_bool(doc, item_val, "{prop_name}", item.{prop_name});'
                )
            elif prim_type == 'number':
                lines.append(
                    f'{prefix}yyjson_mut_obj_add_real(doc, item_val, "{prop_name}", item.{prop_name});'
                )
        
        elif prop_schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, prop_schema)
            
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.extend([
                    f'{prefix}yyjson_mut_val *{prop_name}_obj = item.{prop_name}->ToJSON(doc);',
                    f'{prefix}yyjson_mut_obj_add_val(doc, item_val, "{prop_name}", {prop_name}_obj);'
                ])
            else:
                lines.extend([
                    f'{prefix}yyjson_mut_val *{prop_name}_obj = item.{prop_name}.ToJSON(doc);',
                    f'{prefix}yyjson_mut_obj_add_val(doc, item_val, "{prop_name}", {prop_name}_obj);'
                ])
        
        return lines

    def _serialize_nested_array_item(
        self,
        nested_array: ArrayProperty,
        prefix: str
    ) -> List[str]:
        """Serialize nested array items (array of arrays)"""
        
        lines = [
            f'{prefix}\tyyjson_mut_val *item_val = yyjson_mut_arr(doc);',
            f'{prefix}\tfor (const auto &nested_item : item) {{'
        ]
        
        nested_item_type = nested_array.item_type
        
        if nested_item_type.type == Property.Type.PRIMITIVE:
            prim_nested = cast(PrimitiveProperty, nested_item_type)
            if prim_nested.primitive_type == 'string':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *nested_val = yyjson_mut_str(doc, nested_item.c_str());'
                )
            elif prim_nested.primitive_type == 'integer':
                if prim_nested.format == 'int64':
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *nested_val = yyjson_mut_sint(doc, nested_item);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tyyjson_mut_val *nested_val = yyjson_mut_int(doc, nested_item);'
                    )
            elif prim_nested.primitive_type == 'boolean':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *nested_val = yyjson_mut_bool(doc, nested_item);'
                )
            elif prim_nested.primitive_type == 'number':
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *nested_val = yyjson_mut_real(doc, nested_item);'
                )
        elif nested_item_type.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, nested_item_type)
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *nested_val = nested_item->ToJSON(doc);'
                )
            else:
                lines.append(
                    f'{prefix}\t\tyyjson_mut_val *nested_val = nested_item.ToJSON(doc);'
                )
        
        lines.extend([
            f'{prefix}\t\tyyjson_mut_arr_append(item_val, nested_val);',
            f'{prefix}\t}}'
        ])
        
        return lines

    def _serialize_schema_reference(
        self,
        var_name: str,
        json_name: str,
        prop: SchemaReferenceProperty,
        prefix: str
    ) -> List[str]:
        """Serialize schema reference (nested object)"""
        
        if prop.ref in self.parse_info.recursive_schemas:
            # Recursive schema - use pointer dereference
            return [
                f'{prefix}yyjson_mut_val *{var_name}_val = {var_name}->ToJSON(doc);',
                f'{prefix}yyjson_mut_obj_add_val(doc, obj, "{json_name}", {var_name}_val);'
            ]
        else:
            # Normal schema - call ToJSON directly
            return [
                f'{prefix}yyjson_mut_val *{var_name}_val = {var_name}.ToJSON(doc);',
                f'{prefix}yyjson_mut_obj_add_val(doc, obj, "{json_name}", {var_name}_val);'
            ]

    def _serialize_object(
        self,
        var_name: str,
        json_name: str,
        prop: ObjectProperty,
        prefix: str
    ) -> List[str]:
        """Serialize object/map types"""
        
        if prop.is_raw_object():
            # Raw yyjson_val * - just add it directly
            return [
                f'{prefix}yyjson_mut_obj_add_val(doc, obj, "{json_name}", {var_name});'
            ]
        elif prop.additional_properties:
            # Map type - iterate and add
            lines = [
                f'{prefix}yyjson_mut_val *{var_name}_obj = yyjson_mut_obj(doc);',
                f'{prefix}for (const auto &it : {var_name}) {{',
                f'{prefix}\tauto &key = it.first;',
                f'{prefix}\tauto &value = it.second;',
            ]
            
            # Serialize map values based on their type
            add_prop = prop.additional_properties
            if add_prop.type == Property.Type.PRIMITIVE:
                prim_prop = cast(PrimitiveProperty, add_prop)
                if prim_prop.primitive_type == 'string':
                    lines.append(
                        f'{prefix}\tyyjson_mut_obj_add_str(doc, {var_name}_obj, key.c_str(), value.c_str());'
                    )
                elif prim_prop.primitive_type == 'integer':
                    lines.append(
                        f'{prefix}\tyyjson_mut_obj_add_int(doc, {var_name}_obj, key.c_str(), value);'
                    )
                elif prim_prop.primitive_type == 'boolean':
                    lines.append(
                        f'{prefix}\tyyjson_mut_obj_add_bool(doc, {var_name}_obj, key.c_str(), value);'
                    )
                elif prim_prop.primitive_type == 'number':
                    lines.append(
                        f'{prefix}\tyyjson_mut_obj_add_real(doc, {var_name}_obj, key.c_str(), value);'
                    )
            elif add_prop.type == Property.Type.SCHEMA_REFERENCE:
                schema_ref = cast(SchemaReferenceProperty, add_prop)
                lines.append(
                    f'{prefix}\tyyjson_mut_val *value_obj = value.ToJSON(doc);'
                )
                lines.append(
                    f'{prefix}\tyyjson_mut_obj_add_val(doc, {var_name}_obj, key.c_str(), value_obj);'
                )
            
            lines.extend([
                f'{prefix}}}',
                f'{prefix}yyjson_mut_obj_add_val(doc, obj, "{json_name}", {var_name}_obj);'
            ])
            
            return lines
        
        return [f'{prefix}// TODO: Complex object serialization']

    def _generate_additional_properties_serialization(self) -> List[str]:
        """Serialize additionalProperties map"""
        
        lines = [
            "\t// Serialize additional properties",
            "\tfor (const auto &it : additional_properties) {",
            '\tauto &key = it.first;',
            '\tauto &value = it.second;',
        ]
        
        add_prop = self.additional_properties.schema
        
        if add_prop.type == Property.Type.PRIMITIVE:
            prim_prop = cast(PrimitiveProperty, add_prop)
            if prim_prop.primitive_type == 'string':
                lines.append(
                    "\t\tyyjson_mut_obj_add_str(doc, obj, key.c_str(), value.c_str());"
                )
            elif prim_prop.primitive_type == 'integer':
                if prim_prop.format == 'int64':
                    lines.append(
                        "\t\tyyjson_mut_obj_add_sint(doc, obj, key.c_str(), value);"
                    )
                else:
                    lines.append(
                        "\t\tyyjson_mut_obj_add_int(doc, obj, key.c_str(), value);"
                    )
            elif prim_prop.primitive_type == 'boolean':
                lines.append(
                    "\t\tyyjson_mut_obj_add_bool(doc, obj, key.c_str(), value);"
                )
            elif prim_prop.primitive_type == 'number':
                lines.append(
                    "\t\tyyjson_mut_obj_add_real(doc, obj, key.c_str(), value);"
                )
        elif add_prop.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, add_prop)
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.extend([
                    "\t\tyyjson_mut_val *value_obj = value->ToJSON(doc);",
                    "\t\tyyjson_mut_obj_add_val(doc, obj, key.c_str(), value_obj);"
                ])
            else:
                lines.extend([
                    "\t\tyyjson_mut_val *value_obj = value.ToJSON(doc);",
                    "\t\tyyjson_mut_obj_add_val(doc, obj, key.c_str(), value_obj);"
                ])
        
        lines.extend([
            "\t}",
            ""
        ])
        
        return lines


if __name__ == '__main__':
    openapi_parser = ResponseObjectsGenerator(API_SPEC_PATH)
    openapi_parser.parse_all_schemas()

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_HEADER_DIR, exist_ok=True)
    os.makedirs(OUTPUT_SOURCE_DIR, exist_ok=True)

    with open(os.path.join(OUTPUT_HEADER_DIR, 'list.hpp'), 'w') as f:
        lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]
        # Add includes for all generated headers
        for name in openapi_parser.schemas:
            lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
        f.write('\n'.join(lines))

    with open(os.path.join(OUTPUT_SOURCE_DIR, 'CMakeLists.txt'), 'w') as f:
        file_paths = []
        for name in openapi_parser.schemas:
            file_paths.append(f'\t{to_snake_case(name)}.cpp')
        f.write(CMAKE_LISTS_FORMAT.format(ALL_SOURCE_FILES='\n'.join(file_paths)))

    parse_info = ParseInfo(
        recursive_schemas=openapi_parser.recursive_schemas,
        schemas=openapi_parser.schemas,
        parsed_schemas=openapi_parser.parsed_schemas,
    )

    for name in openapi_parser.schemas:
        schema = openapi_parser.parsed_schemas[name]

        cpp_class = CPPClass(name, parse_info)
        cpp_class.from_property(schema)

        referenced_schemas = cpp_class.get_all_referenced_schemas()
        include_schemas = [x for x in referenced_schemas if x in parse_info.schemas]

        output_path = os.path.join(OUTPUT_HEADER_DIR, f'{to_snake_case(name)}.hpp')
        with open(output_path, 'w') as f:
            content = cpp_class.write_header()
            forward_declarations = [
                f'class {x};' for x in sorted(list(include_schemas)) if x in parse_info.recursive_schemas
            ]
            additional_headers = [
                f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"'
                for x in sorted(list(include_schemas))
                if x not in parse_info.recursive_schemas
            ]
            file_content = HEADER_FORMAT.format(
                ADDITIONAL_HEADERS='\n'.join(additional_headers),
                FORWARD_DECLARATIONS='\n'.join(forward_declarations),
                CLASS_DECLARATION='\n'.join(content),
            )
            f.write(file_content)

        output_path = os.path.join(OUTPUT_SOURCE_DIR, f'{to_snake_case(name)}.cpp')
        with open(output_path, 'w') as f:
            content = cpp_class.write_source([])
            additional_headers = [
                f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"' for x in sorted(list(include_schemas))
            ]
            file_content = SOURCE_FORMAT.format(
                HEADER_NAME=to_snake_case(name),
                ADDITIONAL_HEADERS='\n'.join(additional_headers),
                CLASS_DEFINITION='\n'.join(content),
            )
            f.write(file_content)
