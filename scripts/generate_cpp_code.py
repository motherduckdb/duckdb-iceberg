from parse_openapi_spec import (
    ResponseObjectsGenerator,
    Property,
    ArrayProperty,
    PrimitiveProperty,
    SchemaReferenceProperty,
    ObjectProperty,
)
import os
import json
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

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
{ADDITIONAL_HEADERS}

namespace duckdb {{
namespace rest_api_objects {{

{FORWARD_DECLARATIONS}

{CLASS_DECLARATION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

SOURCE_FORMAT = """
#include "rest_catalog/objects/{HEADER_NAME}.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {{
namespace rest_api_objects {{

{CLASS_DEFINITION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

JSON_UTILS_HEADER_FORMAT = """
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {{
namespace rest_api_objects {{
namespace json_utils {{

inline bool IsNull(const JSONValue &value) {{
	return value.IsNull();
}}

inline void *GetNull(const JSONValue &value) {{
	return nullptr;
}}

inline bool IsString(const JSONValue &value) {{
	return value.IsString();
}}

inline string GetString(const JSONValue &value) {{
	return value.GetString();
}}

inline bool IsInteger(const JSONValue &value) {{
	return value.IsInteger();
}}

inline bool IsUnsignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER;
}}

inline bool IsBoolean(const JSONValue &value) {{
	return value.GetType() == JSONValueType::BOOLEAN;
}}

inline bool GetBoolean(const JSONValue &value) {{
	return value.GetBoolean();
}}

inline bool IsNumber(const JSONValue &value) {{
	return value.IsInteger() || value.GetType() == JSONValueType::DOUBLE;
}}

inline int64_t GetSignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::SIGNED_INTEGER ? value.GetSignedInteger()
	                                                        : static_cast<int64_t>(value.GetUnsignedInteger());
}}

inline uint64_t GetUnsignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER
	           ? value.GetUnsignedInteger()
	           : static_cast<uint64_t>(value.GetSignedInteger());
}}

inline double GetNumber(const JSONValue &value) {{
	return value.IsInteger() ? static_cast<double>(GetSignedInteger(value)) : value.GetDouble();
}}

inline string GetTypeDescription(const JSONValue &value) {{
	return StringUtil::Format("JSON type %d", static_cast<int>(value.GetType()));
}}

}} // namespace json_utils
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
    discriminator_value: Optional[str] = None


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
    uses_optional_wrapper: bool


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
    uses_optional_wrapper: bool = False


@dataclass
class PrimitiveTypeMapping:
    conversion: str
    type_check: str
    cpp_type: str
    formats: Dict[str, "PrimitiveTypeMapping"] = field(default_factory=dict)


PRIMITIVE_TYPE_MAPPING = {
    None: PrimitiveTypeMapping(type_check='json_utils::IsNull', conversion='json_utils::GetNull', cpp_type='void*'),
    'string': PrimitiveTypeMapping(
        type_check='json_utils::IsString', conversion='json_utils::GetString', cpp_type='string'
    ),
    'integer': PrimitiveTypeMapping(
        type_check='json_utils::IsInteger',
        conversion='json_utils::GetSignedInteger',
        cpp_type='int32_t',
        formats={
            'int64': PrimitiveTypeMapping(
                type_check='json_utils::IsInteger', conversion='json_utils::GetSignedInteger', cpp_type='int64_t'
            )
        },
    ),
    'boolean': PrimitiveTypeMapping(
        type_check='json_utils::IsBoolean', conversion='json_utils::GetBoolean', cpp_type='bool'
    ),
    'number': PrimitiveTypeMapping(
        type_check='json_utils::IsNumber', conversion='json_utils::GetNumber', cpp_type='double'
    ),
}


class CPPClass:
    def __init__(self, class_name, parse_info: ParseInfo):
        self.name = class_name
        self.parse_info = parse_info
        # The base classes that make up this class
        self.one_of: List[OneOf] = []
        self.all_of: List[AllOf] = []
        self.any_of: List[AnyOf] = []
        self.discriminator_property: Optional[str] = None

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
        uses_optional_wrapper: bool = False,
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
                uses_optional_wrapper=uses_optional_wrapper,
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
        refinement_body = self.generate_inherited_property_refinements(object_property, inherited_properties)

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
        self.try_from_json_body = refinement_body + res
        self.generate_nested_class_definitions()

    def generate_inherited_property_refinements(
        self, object_property: ObjectProperty, inherited_properties: Set[str]
    ) -> List[str]:
        result = []
        required = set(object_property.required or [])
        for property_name in object_property.properties.keys() & inherited_properties:
            property_schema = object_property.properties[property_name]
            variable_name = safe_cpp_name(property_name) + '_refinement'
            value_name = variable_name + '_val'
            result.append(f'auto {value_name} = obj.GetMember("{property_name}");')
            result.append(f'if ({value_name}.IsValid()) {{')
            result.append(f'\t{self.generate_variable_type(property_schema)} {variable_name};')
            assignment = self.generate_assignment(property_schema, variable_name, value_name, True)
            result.extend([f'\t{x}' for x in assignment])
            if property_name in required:
                result.extend(
                    [
                        '} else {',
                        f'''\treturn "{self.name} required property '{property_name}' is missing";''',
                        '}',
                    ]
                )
            else:
                result.append('}')
        return result

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
                f'auto {required_property.variable_name}_val = obj.GetMember("{required_property.property_name}");',
                f'if (!{required_property.variable_name}_val.IsValid()) {{',
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
        res.extend(
            [
                f'auto {optional_property.variable_name}_val = obj.GetMember("{optional_property.property_name}");',
                f'if ({optional_property.variable_name}_val.IsValid()) {{',
            ]
        )
        if optional_property.nullable:
            res.extend(
                [
                    f'\tif ({optional_property.variable_name}_val.IsNull()) {{',
                    '\t\t//! do nothing, property is explicitly nullable',
                    '\t} else {',
                ]
            )
            res.extend([f'\t\t{x}' for x in optional_property.body])
            res.append('\t}')
        else:
            res.extend([f'\t{x}' for x in optional_property.body])
        res.append('}')
        return res

    @staticmethod
    def make_callback_safe(lines: List[str]) -> List[str]:
        result = []
        for line in lines:
            statement = line.strip()
            if statement.startswith('return ') and statement.endswith(';'):
                indent = line[: len(line) - len(line.lstrip())]
                expression = statement[len('return ') : -1]
                if expression != 'error':
                    result.append(f'{indent}error = {expression};')
                result.append(f'{indent}return;')
            elif statement == 'continue;':
                indent = line[: len(line) - len(line.lstrip())]
                result.append(f'{indent}return;')
            else:
                result.append(line)
        return result

    def write_additional_properties(self) -> List[str]:
        if not self.additional_properties:
            return []
        res = []

        res.extend(self.additional_properties.exclude_list)
        res.append('obj.IterateObject([&](const string &key_str, JSONValue val) {')
        res.append('\tif (!error.empty()) {')
        res.append('\t\treturn;')
        res.append('\t}')
        res.extend(self.make_callback_safe(self.additional_properties.skip_if_excluded))
        res.extend(self.make_callback_safe(self.additional_properties.body))
        res.extend(
            [
                '\tadditional_properties.emplace(key_str, std::move(tmp));',
                '});',
                'if (!error.empty()) {',
                '\treturn error;',
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
        if self.discriminator_property and all(item.discriminator_value is not None for item in self.one_of):
            res = [
                f'auto discriminator_val = obj.GetMember("{self.discriminator_property}");',
                'if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {',
                f'''\treturn "{self.name} discriminator '{self.discriminator_property}' is missing or is not a string";''',
                '}',
                'string discriminator = discriminator_val.GetString();',
            ]
            for index, item in enumerate(self.one_of):
                prefix = 'if' if index == 0 else 'else if'
                res.append(f'{prefix} (discriminator == {json.dumps(item.discriminator_value)}) {{')
                is_recursive = item.class_name in self.parse_info.recursive_schemas
                if is_recursive:
                    res.append(f'\t{item.name} = make_uniq<{item.class_name}>();')
                else:
                    res.append(f'\t{item.name}.emplace();')
                res.extend(
                    [
                        f'\terror = {item.name}->TryFromJSON(obj);',
                        '\tif (!error.empty()) {',
                        '\t\treturn error;',
                        '\t}',
                        '}',
                    ]
                )
            res.extend(
                [
                    'else {',
                    f'''\treturn StringUtil::Format("{self.name} has unknown discriminator value '%s'", discriminator.c_str());''',
                    '}',
                ]
            )
            return res

        res = []
        res.append('do {')
        for item in self.one_of:
            is_recursive = item.class_name in self.parse_info.recursive_schemas
            if is_recursive:
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            else:
                res.append(f'{item.name}.emplace();')
            res.extend(
                [
                    f'error = {item.name}->TryFromJSON(obj);',
                    'if (error.empty()) {',
                    '\tbreak;',
                    '} else {',
                    f'\t{item.name} = {"nullptr" if is_recursive else "nullopt"};',
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

        all_options = sorted(
            [
                f'!({self.presence_condition(item.name, item.class_name not in self.parse_info.recursive_schemas)})'
                for item in self.any_of
            ]
        )
        condition = ' && '.join(all_options)

        for item in self.any_of:
            is_recursive = item.class_name in self.parse_info.recursive_schemas
            if is_recursive:
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            else:
                res.append(f'{item.name}.emplace();')
            res.extend(
                [
                    f'error = {item.name}->TryFromJSON(obj);',
                    'if (error.empty()) {',
                    '} else {',
                    f'\t{item.name} = {"nullptr" if is_recursive else "nullopt"};',
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
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            if self.schema_is_directly_copyable(array_property.item_type):
                return source
            print(f"Unhandled array copy expression for '{source}'")
            exit(1)
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

    def schema_is_directly_copyable(self, schema: Property) -> bool:
        if schema.type == Property.Type.PRIMITIVE:
            return True
        if schema.type == Property.Type.ARRAY:
            return self.schema_is_directly_copyable(cast(ArrayProperty, schema).item_type)
        if schema.type == Property.Type.OBJECT:
            return cast(ObjectProperty, schema).is_raw_object()
        return False

    def uses_pointer_storage(self, schema: Property) -> bool:
        return (
            schema.type == Property.Type.SCHEMA_REFERENCE
            and cast(SchemaReferenceProperty, schema).ref in self.parse_info.recursive_schemas
        )

    def uses_optional_wrapper(self, schema: Property) -> bool:
        return not self.uses_pointer_storage(schema)

    def presence_condition(self, variable_name: str, uses_optional_wrapper: bool) -> str:
        if uses_optional_wrapper:
            return f'{variable_name}.has_value()'
        return f'{variable_name} != nullptr'

    def optional_member_type(self, schema: Property) -> str:
        variable_type = self.generate_variable_type(schema)
        if self.uses_optional_wrapper(schema):
            return f'optional<{variable_type}>'
        return variable_type

    def value_access_expression(self, variable_name: str, uses_optional_wrapper: bool) -> str:
        if uses_optional_wrapper:
            return f'(*{variable_name})'
        return variable_name

    def generate_optional_assignment(self, schema: Property, target: str, source: str) -> List[str]:
        if self.uses_optional_wrapper(schema):
            tmp_name = f'{target}_tmp'
            variable_type = self.generate_variable_type(schema)
            res = [f'{variable_type} {tmp_name};']
            res.extend(self.generate_assignment(schema, tmp_name, source, True, handle_nullable=False))
            res.append(f'{target} = std::move({tmp_name});')
            return res
        return self.generate_assignment(schema, target, source, True, handle_nullable=False)

    def generate_nullable_assignment(self, schema: Property, target: str, source: str) -> List[str]:
        uses_optional_wrapper = self.uses_optional_wrapper(schema)
        result = [f'if ({source}.IsNull()) {{']
        result.append(f'\t{target} = {"nullopt" if uses_optional_wrapper else "nullptr"};')
        result.append('} else {')
        if uses_optional_wrapper:
            temporary = f'{target}_tmp'
            result.append(f'\t{self.generate_variable_type(schema)} {temporary};')
            assignment = self.generate_assignment(
                schema, temporary, source, True, handle_nullable=False
            )
            result.extend([f'\t{x}' for x in assignment])
            result.append(f'\t{target} = std::move({temporary});')
        else:
            assignment = self.generate_assignment(schema, target, source, True, handle_nullable=False)
            result.extend([f'\t{x}' for x in assignment])
        result.append('}')
        return result

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
            target = f'res.{member.variable_name}'
            source = member.variable_name
            if member.uses_optional_wrapper:
                lines = [f'{target}.emplace();']
                lines.extend(
                    self.write_copy_assignment_lines(
                        self.value_access_expression(target, True),
                        self.value_access_expression(source, True),
                        member.schema,
                    )
                )
            else:
                lines = self.write_copy_assignment_lines(target, source, member.schema)
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
                f'{qualified_name} {qualified_name}::FromJSON(JSONValue obj) {{',
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
                f'string {qualified_name}::TryFromJSON(JSONValue obj) {{',
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

        # Serialization methods
        if supports_population:
            res.extend([''])
            res.extend(self.generate_populate_json_method(qualified_name))
            res.extend([''])
        res.extend(self.generate_to_json_method(qualified_name))
        return res

    def write_header(self) -> List[str]:
        res = []
        supports_population = self.supports_json_object_population()
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
                f'\tstatic {self.name} FromJSON(JSONValue obj);',
                '\tstring TryFromJSON(JSONValue obj);',
                '',
                '\t// Copy',
                f'\t{self.name} Copy() const;',
                '',
                '\t// Serialization',
            ]
        )
        if supports_population:
            res.append('\tvoid PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;')
        res.extend([
            '\tJSONMutableValue ToJSON(JSONWriter &writer) const;',
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

    def generate_any_of_items(self, items: List[Property]) -> None:
        if not items:
            return
        for item in items:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'
            uses_optional_wrapper = item.ref not in self.parse_info.recursive_schemas

            self.any_of.append(AnyOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.add_member(
                property_name,
                self.optional_member_type(item),
                item,
                copy_guard=self.presence_condition(property_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_any_of(self, property: Property):
        self.generate_any_of_items(property.any_of)

    def composition_items_are_primitive(self, items: List[Property]) -> bool:
        if not items:
            return False
        for item in items:
            if item.type != Property.Type.SCHEMA_REFERENCE:
                return False
            if self.parse_info.parsed_schemas[item.ref].type != Property.Type.PRIMITIVE:
                return False
        return True

    def generate_one_of(self, property: Property):
        if not property.one_of:
            return
        # Primitive Iceberg values have semantic alternatives that intentionally
        # overlap at the JSON-type level (for example int/long and many strings).
        # Preserve all matching views so the caller's Iceberg type can select one.
        if self.composition_items_are_primitive(property.one_of):
            self.generate_any_of_items(property.one_of)
            return

        discriminator_mapping = {}
        if property.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, property)
            if object_property.discriminator:
                self.discriminator_property = object_property.discriminator.get('propertyName')
                discriminator_mapping = {
                    mapped_ref.split('/')[-1]: value
                    for value, mapped_ref in object_property.discriminator.get('mapping', {}).items()
                }
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'
            uses_optional_wrapper = item.ref not in self.parse_info.recursive_schemas

            self.one_of.append(
                OneOf(
                    name=property_name,
                    dereference_style=dereference_style,
                    class_name=class_name,
                    discriminator_value=discriminator_mapping.get(class_name),
                )
            )
            self.add_member(
                property_name,
                self.optional_member_type(item),
                item,
                copy_guard=self.presence_condition(property_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_array_loop(
        self, array_name, destination_name, array_property: ArrayProperty, handle_nullable: bool = True
    ) -> List[str]:
        item_type = array_property.item_type
        item_name = f'{destination_name}_item'
        item_value_name = f'{destination_name}_item_val'
        body = []
        body.append(f'{array_name}.IterateArray([&](JSONValue {item_value_name}) {{')
        body.append('\tif (!error.empty()) {')
        body.append('\t\treturn;')
        body.append('\t}')

        assignment = f'std::move({item_name})'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'{self.generate_variable_type(item_type)} {item_name};')
            body.extend(self.make_callback_safe(self.generate_assignment(item_type, item_name, item_value_name, True)))
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            self.referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.parse_info.recursive_schemas:
                item_pointer_name = f'{item_name}_p'
                body.extend(
                    [
                        f'\tauto {item_pointer_name} = make_uniq<{schema_property.ref}>();',
                        f'\tauto &{item_name} = *{item_pointer_name};',
                    ]
                )
                assignment = f'std::move({item_pointer_name})'
            else:
                body.append(f'\t{schema_property.ref} {item_name};')
            body.extend(self.make_callback_safe(
                [
                    f'\terror = {item_name}.TryFromJSON({item_value_name});',
                    '\tif (!error.empty()) {',
                    '\t\treturn error;',
                    '\t}',
                ]
            ))
        body.append(f'\t{destination_name}.emplace_back({assignment});')
        body.append('});')
        body.append('if (!error.empty()) {')
        body.append('\treturn error;')
        body.append('}')

        res = []
        prefix = ''
        if handle_nullable and array_property.nullable is not None:
            prefix = '} else '
            if array_property.nullable == True:
                res.extend(
                    [f'if ({array_name}.IsNull()) {{', '\t//! do nothing, property is explicitly nullable']
                )
            else:
                res.extend(
                    [
                        f'if ({array_name}.IsNull()) {{',
                        f'''\treturn "{self.name} property '{destination_name}' is not nullable, but is 'null'";''',
                    ]
                )

        res.append(f'{prefix}if ({array_name}.IsArray()) {{')
        res.extend([f'\t{x}' for x in body])
        res.extend(
            [
                '} else {',
                f"""\treturn StringUtil::Format("{self.name} property '{destination_name}' is not of type 'array', found %s instead", json_utils::GetTypeDescription({array_name}).c_str());""",
                '}',
            ]
        )

        return res

    def generate_item_parse(
        self, property: Property, source: str, target: str, is_required: bool, handle_nullable: bool = True
    ) -> List[str]:
        res = []
        prefix = ''
        if handle_nullable and property.nullable is not None:
            prefix = '} else '
            if property.nullable == True:
                res.extend(
                    [
                        f'if ({source}.IsNull()) {{',
                        '\t//! do nothing, property is explicitly nullable',
                    ]
                )
            else:
                res.extend(
                    [
                        f'if ({source}.IsNull()) {{',
                        f'''\treturn "{self.name} property '{target}' is not nullable, but is 'null'";''',
                    ]
                )

        if property.type == Property.Type.SCHEMA_REFERENCE:
            print(f"Unrecognized property type {property.type}, {source}")
            exit(1)
        if property.type == Property.Type.ARRAY:
            return self.generate_array_loop(
                source, target, cast(ArrayProperty, property), handle_nullable=handle_nullable
            )
        elif property.type == Property.Type.PRIMITIVE:
            # Validate the JSON type before extracting the value.
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
                        f'}} else if (json_utils::IsUnsignedInteger({source})) {{',
                        f'\t{target} = json_utils::GetUnsignedInteger({source});',
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
                    f"""\treturn StringUtil::Format("{self.name} property '{target}' is not of type '{item_type}', found %s instead", json_utils::GetTypeDescription({source}).c_str());""",
                    '}',
                ]
            )
            if primitive_property.const is not None:
                if isinstance(primitive_property.const, str):
                    const_literal = json.dumps(primitive_property.const)
                elif isinstance(primitive_property.const, bool):
                    const_literal = 'true' if primitive_property.const else 'false'
                else:
                    const_literal = str(primitive_property.const)
                res.extend(
                    [
                        f'if (!{source}.IsNull() && {target} != {const_literal}) {{',
                        f'''\treturn "{self.name} property '{target}' does not match its required const value";''',
                        '}',
                    ]
                )
        elif property.type == Property.Type.OBJECT and property.is_raw_object():
            res.extend(
                [
                    f'{prefix}if ({source}.IsObject()) {{',
                    f'\t{target} = {source};',
                    '} else {',
                    f"""\treturn "{self.name} property '{target}' is not of type 'object'";""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.additional_properties:
            object_property = cast(ObjectProperty, property)
            additional_properties = property.additional_properties

            res.append(f'{prefix}if ({source}.IsObject()) {{')
            res.append(f'\t{source}.IterateObject([&](const string &key_str, JSONValue val) {{')
            res.append('\t\tif (!error.empty()) {')
            res.append('\t\t\treturn;')
            res.append('\t\t}')
            res.append(f'\t\t{self.generate_variable_type(additional_properties)} tmp;')

            if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
                item_definition = [
                    f'\t\t{x}'
                    for x in self.make_callback_safe(
                        self.generate_item_parse(additional_properties, 'val', 'tmp', True)
                    )
                ]
                res.extend(item_definition)
            else:
                schema_property = cast(SchemaReferenceProperty, additional_properties)
                self.referenced_schemas.add(schema_property.ref)
                if schema_property.ref in self.parse_info.recursive_schemas:
                    print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                    exit(1)
                res.append(f'\t\t{schema_property.ref} tmp;')
                res.extend(self.make_callback_safe(
                    [
                        '\t\terror = tmp.TryFromJSON(val);',
                        '\t\tif (!error.empty()) {',
                        '\t\t\treturn error;',
                        '\t\t}',
                    ]
                ))
            res.extend(
                [
                    f'\t\t{target}.emplace(key_str, std::move(tmp));',
                    '\t});',
                    '\tif (!error.empty()) {',
                    '\t\treturn error;',
                    '\t}',
                ]
            )
            res.extend(['} else {', f"""\treturn "{self.name} property '{target}' is not of type 'object'";""", '}'])
        else:
            print(f"Unrecognized type in 'generate_item_parse', {property.type}")
            exit(1)
        return res

    def generate_assignment(
        self, schema: Property, target: str, source: str, is_required: bool, handle_nullable: bool = True
    ) -> List[str]:
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            return self.generate_array_loop(source, target, array_property, handle_nullable=handle_nullable)
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
            return self.generate_item_parse(schema, source, target, is_required, handle_nullable=handle_nullable)

    def generate_optional_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, optional_property in properties.items():
            variable_name = safe_cpp_name(item)
            uses_optional_wrapper = self.uses_optional_wrapper(optional_property)
            body = self.generate_optional_assignment(optional_property, variable_name, f'{variable_name}_val')
            self.optional_properties[item] = OptionalProperty(
                property_name=item,
                variable_name=variable_name,
                body=body,
                schema=optional_property,
                nullable=optional_property.nullable,
                uses_optional_wrapper=uses_optional_wrapper,
            )
            variable_type = self.optional_member_type(optional_property)
            self.add_member(
                variable_name,
                variable_type,
                optional_property,
                copy_guard=self.presence_condition(variable_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_required_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            is_nullable = required_property.nullable is True
            uses_optional_wrapper = is_nullable and self.uses_optional_wrapper(required_property)
            if is_nullable:
                body = self.generate_nullable_assignment(
                    required_property, variable_name, f'{variable_name}_val'
                )
            else:
                body = self.generate_assignment(required_property, variable_name, f'{variable_name}_val', True)
            if required_property.default is not None:
                default = [f'{variable_name} = "{str(required_property.default)}";']
            else:
                default = None
            self.required_properties[item] = RequiredProperty(
                property_name=item, variable_name=variable_name, body=body, default=default, schema=required_property
            )
            variable_type = (
                self.optional_member_type(required_property)
                if is_nullable
                else self.generate_variable_type(required_property)
            )
            self.add_member(
                variable_name,
                variable_type,
                required_property,
                copy_guard=(
                    self.presence_condition(variable_name, uses_optional_wrapper)
                    if is_nullable
                    else None
                ),
                uses_optional_wrapper=uses_optional_wrapper,
            )

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
            return 'JSONValue'
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
        return self.schema_supports_json_object_population(
            self.parse_info.parsed_schemas[class_name]
        )

    def variant_uses_optional_wrapper(self, class_name: str) -> bool:
        return class_name not in self.parse_info.recursive_schemas

    def variant_presence_condition(self, variant_name: str, class_name: str) -> str:
        return self.presence_condition(variant_name, self.variant_uses_optional_wrapper(class_name))

    def _generate_json_object_merge(self, source_expr: str, temp_name: str, indent: int = 1) -> List[str]:
        prefix = '\t' * indent
        return [
            f'{prefix}(void){source_expr};',
            f'{prefix}throw InternalException("PopulateJSON requires an object-like JSON value");',
        ]

    def generate_populate_json_method(self, qualified_name: str) -> List[str]:
        lines = [
            f"void {qualified_name}::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {{",
            "",
        ]

        if self.one_of:
            for i, variant in enumerate(self.one_of):
                if i == 0:
                    lines.append(f"\tif ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")
                else:
                    lines.append(f"\t}} else if ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    lines.append(f"\t\t{variant.name}->PopulateJSON(writer, obj);")
                else:
                    accessor = f"{variant.name}->ToJSON(writer)"
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
                    lines.append(f"\tif ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")
                else:
                    lines.append(f"\t}} else if ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    lines.append(f"\t\t{variant.name}->PopulateJSON(writer, obj);")
                else:
                    accessor = f"{variant.name}->ToJSON(writer)"
                    lines.extend(self._generate_json_object_merge(accessor, f"{variant.name}_obj", indent=2))

            lines.extend([
                "\t}",
                "}",
            ])
            return lines

        if self.any_of:
            for i, variant in enumerate(self.any_of):
                if i == 0:
                    lines.append(f"\tif ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")
                else:
                    lines.append(f"\t}} else if ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")

                if self.class_supports_json_object_population(variant.class_name):
                    lines.append(f"\t\t{variant.name}->PopulateJSON(writer, obj);")
                else:
                    accessor = f"{variant.name}->ToJSON(writer)"
                    lines.extend(self._generate_json_object_merge(accessor, f"{variant.name}_obj", indent=2))

            lines.append("\t}")
            lines.append("")

        if self.all_of:
            for base in self.all_of:
                if base.class_name:
                    lines.append(f"\t// Serialize base class: {base.class_name}")
                    if self.class_supports_json_object_population(base.class_name):
                        lines.append(f"\t{base.name}.PopulateJSON(writer, obj);")
                    else:
                        lines.extend(
                            self._generate_json_object_merge(
                                f"{base.name}.ToJSON(writer)", f"{base.name}base_obj", indent=1
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
                f"JSONMutableValue {qualified_name}::ToJSON(JSONWriter &writer) const {{"
            ]

            if prim_type is None:
                lines.append("\treturn writer.CreateNull();")
            elif prim_type == 'string':
                lines.append("\treturn writer.CreateString(value);")
            elif prim_type == 'integer':
                if prim.format == 'int64':
                    lines.append("\treturn writer.CreateSignedInteger(value);")
                else:
                    lines.append("\treturn writer.CreateSignedInteger(value);")
            elif prim_type == 'boolean':
                lines.append("\treturn writer.CreateBoolean(value);")
            elif prim_type == 'number':
                lines.append("\treturn writer.CreateDouble(value);")
            else:
                lines.append('\tthrow InternalException("Unsupported primitive serialization");')

            lines.append("}")
            return lines

        if root_schema and root_schema.type == Property.Type.ARRAY:
            array_schema = cast(ArrayProperty, root_schema)
            lines = [
                f"JSONMutableValue {qualified_name}::ToJSON(JSONWriter &writer) const {{",
                "\tauto arr = writer.CreateArray();",
                "\tfor (const auto &item : value) {"
            ]

            item_type = array_schema.item_type
            if item_type.type == Property.Type.PRIMITIVE:
                prim_item = cast(PrimitiveProperty, item_type)
                if prim_item.primitive_type == 'string':
                    lines.append("\t\tarr.AppendString(item);")
                elif prim_item.primitive_type == 'integer':
                    if prim_item.format == 'int64':
                        lines.append("\t\tarr.Append(writer.CreateSignedInteger(item));")
                    else:
                        lines.append("\t\tarr.Append(writer.CreateSignedInteger(item));")
                elif prim_item.primitive_type == 'boolean':
                    lines.append("\t\tarr.Append(writer.CreateBoolean(item));")
                elif prim_item.primitive_type == 'number':
                    lines.append("\t\tarr.Append(writer.CreateDouble(item));")
            elif item_type.type == Property.Type.SCHEMA_REFERENCE:
                schema_ref = cast(SchemaReferenceProperty, item_type)
                if schema_ref.ref in self.parse_info.recursive_schemas:
                    lines.append("\t\tarr.Append(item->ToJSON(writer));")
                else:
                    lines.append("\t\tarr.Append(item.ToJSON(writer));")

            lines.extend([
                "\t}",
                "\treturn arr;",
                "}"
            ])
            return lines

        if supports_population:
            return [
                f"JSONMutableValue {qualified_name}::ToJSON(JSONWriter &writer) const {{",
                "\tauto obj = writer.CreateObject();",
                "\tPopulateJSON(writer, obj);",
                "\treturn obj;",
                "}",
            ]

        lines = []
        lines.extend([
            f"JSONMutableValue {qualified_name}::ToJSON(JSONWriter &writer) const {{",
        ])

        if self.one_of:
            for i, variant in enumerate(self.one_of):
                if i == 0:
                    lines.append(f"\tif ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")
                else:
                    lines.append(f"\t}} else if ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")

                lines.append(f"\t\treturn {variant.name}->ToJSON(writer);")

            lines.extend([
                "\t}",
                "\t// No variant is active - return empty object",
                "\treturn writer.CreateObject();",
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
                    lines.append(f"\tif ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")
                else:
                    lines.append(f"\t}} else if ({self.variant_presence_condition(variant.name, variant.class_name)}) {{")

                lines.append(f"\t\treturn {variant.name}->ToJSON(writer);")

            lines.extend([
                "\t}",
                "\t// No variant is active - return null"
                if any_of_is_primitive
                else "\t// No variant is active - return empty object",
                "\treturn writer.CreateNull();" if any_of_is_primitive else "\treturn writer.CreateObject();",
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
        
        if not required or property_schema.nullable is True:
            uses_optional_wrapper = self.uses_optional_wrapper(property_schema)
            lines.append(f"\tif ({self.presence_condition(var_name, uses_optional_wrapper)}) {{")
            serialization_var_name = var_name
            if uses_optional_wrapper:
                serialization_var_name = f"{var_name}_value"
                lines.append(f"\t\tauto &{serialization_var_name} = *{var_name};")
            inner_lines = self._serialize_value(
                serialization_var_name, json_name, property_schema, indent=2
            )
            lines.extend(inner_lines)
            lines.append("\t}")
            if required:
                lines.extend(
                    [
                        "\telse {",
                        f'\t\tobj.Add("{json_name}", writer.CreateNull());',
                        "\t}",
                    ]
                )
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
                f'{prefix}obj.AddString("{json_name}", {var_name});'
            ]
        elif prim_type == 'integer':
            if prop.format == 'int64':
                return [
                    f'{prefix}obj.Add("{json_name}", writer.CreateSignedInteger({var_name}));'
                ]
            else:
                return [
                    f'{prefix}obj.Add("{json_name}", writer.CreateSignedInteger({var_name}));'
                ]
        elif prim_type == 'boolean':
            return [
                f'{prefix}obj.Add("{json_name}", writer.CreateBoolean({var_name}));'
            ]
        elif prim_type == 'number':
            return [
                f'{prefix}obj.Add("{json_name}", writer.CreateDouble({var_name}));'
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
            f'{prefix}auto {var_name}_arr = writer.CreateArray();',
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
                    f'{prefix}\tauto item_val = item->ToJSON(writer);'
                )
            else:
                lines.append(
                    f'{prefix}\tauto item_val = item.ToJSON(writer);'
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
            f'{prefix}\t{var_name}_arr.Append(item_val);',
            f'{prefix}}}',
            f'{prefix}obj.Add("{json_name}", {var_name}_arr);'
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
                f'{prefix}\tauto item_val = writer.CreateString(item);'
            ]
        elif prim_type == 'integer':
            if prim_prop.format == 'int64':
                return [
                    f'{prefix}\tauto item_val = writer.CreateSignedInteger(item);'
                ]
            else:
                return [
                    f'{prefix}\tauto item_val = writer.CreateSignedInteger(item);'
                ]
        elif prim_type == 'boolean':
            return [
                f'{prefix}\tauto item_val = writer.CreateBoolean(item);'
            ]
        elif prim_type == 'number':
            return [
                f'{prefix}\tauto item_val = writer.CreateDouble(item);'
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
                f'{prefix}\tauto item_val = writer.CreateObject();'
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
                f'{prefix}\tauto item_val = writer.CreateObject();'
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
            f'{prefix}\tauto item_val = writer.CreateObject();'
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
            lines.extend([
                f'{prefix}\titem_val.AddString(key, value);'
            ])
        elif prim_type == 'integer':
            if prim_prop.format == 'int64':
                lines.extend([
                    f'{prefix}\titem_val.Add(key, writer.CreateSignedInteger(value));'
                ]
                )
            else:
                lines.extend([
                    f'{prefix}\titem_val.Add(key, writer.CreateSignedInteger(value));'
                ]
                )
        elif prim_type == 'boolean':
            lines.extend([
                f'{prefix}\titem_val.Add(key, writer.CreateBoolean(value));'
            ]
            )
        elif prim_type == 'number':
            lines.extend([
                f'{prefix}\titem_val.Add(key, writer.CreateDouble(value));'
            ]
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
                f'{prefix}\tauto value_obj = value->ToJSON(writer);',
                f'{prefix}\titem_val.Add(key, value_obj);'
            ])
        else:
            lines.extend([
                f'{prefix}\tauto value_obj = value.ToJSON(writer);',
                f'{prefix}\titem_val.Add(key, value_obj);'
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
            f'{prefix}\tauto value_arr = writer.CreateArray();'
        ]
        
        item_type = array_prop.item_type
        
        if item_type.type == Property.Type.PRIMITIVE:
            prim_item = cast(PrimitiveProperty, item_type)
            lines.append(f'{prefix}\tfor (const auto &arr_item : value_array) {{')
            
            prim_type = prim_item.primitive_type
            if prim_type == 'string':
                lines.append(
                    f'{prefix}\t\tauto arr_item_val = writer.CreateString(arr_item);'
                )
            elif prim_type == 'integer':
                if prim_item.format == 'int64':
                    lines.append(
                        f'{prefix}\t\tauto arr_item_val = writer.CreateSignedInteger(arr_item);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tauto arr_item_val = writer.CreateSignedInteger(arr_item);'
                    )
            elif prim_type == 'boolean':
                lines.append(
                    f'{prefix}\t\tauto arr_item_val = writer.CreateBoolean(arr_item);'
                )
            elif prim_type == 'number':
                lines.append(
                    f'{prefix}\t\tauto arr_item_val = writer.CreateDouble(arr_item);'
                )
            
            lines.extend([
                f'{prefix}\t\tvalue_arr.Append(arr_item_val);',
                f'{prefix}\t}}'
            ])
            
        elif item_type.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, item_type)
            lines.append(f'{prefix}\tfor (const auto &arr_item : value_array) {{')
            
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.append(
                    f'{prefix}\t\tauto arr_item_val = arr_item->ToJSON(writer);'
                )
            else:
                lines.append(
                    f'{prefix}\t\tauto arr_item_val = arr_item.ToJSON(writer);'
                )
            
            lines.extend([
                f'{prefix}\t\tvalue_arr.Append(arr_item_val);',
                f'{prefix}\t}}'
            ])
        
        lines.extend([
            f'{prefix}\titem_val.Add(key, value_arr);',
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
                f'{prefix}\tauto value_obj = writer.CreateCopy(value_map);',
                f'{prefix}\titem_val.Add(key, value_obj);'
            ])
        elif object_prop.additional_properties:
            lines.append(
                f'{prefix}\tauto value_obj = writer.CreateObject();'
            )
            
            nested_value_type = object_prop.additional_properties
            
            if nested_value_type.type == Property.Type.PRIMITIVE:
                nested_prim = cast(PrimitiveProperty, nested_value_type)
                lines.append(
                    f'{prefix}\tfor (const auto &[nested_key, nested_value] : value_map) {{'
                )
                
                if nested_prim.primitive_type == 'string':
                    lines.extend([
                        f'{prefix}\t\tvalue_obj.AddString(nested_key, nested_value);'
                    ]
                    )
                elif nested_prim.primitive_type == 'integer':
                    if nested_prim.format == 'int64':
                        lines.extend([
                            f'{prefix}\t\tvalue_obj.Add(nested_key, writer.CreateSignedInteger(nested_value));'
                        ]
                        )
                    else:
                        lines.extend([
                            f'{prefix}\t\tvalue_obj.Add(nested_key, writer.CreateSignedInteger(nested_value));'
                        ]
                        )
                elif nested_prim.primitive_type == 'boolean':
                    lines.extend([
                        f'{prefix}\t\tvalue_obj.Add(nested_key, writer.CreateBoolean(nested_value));'
                    ]
                    )
                elif nested_prim.primitive_type == 'number':
                    lines.extend([
                        f'{prefix}\t\tvalue_obj.Add(nested_key, writer.CreateDouble(nested_value));'
                    ]
                    )
                
                lines.append(f'{prefix}\t}}')
            
            elif nested_value_type.type == Property.Type.SCHEMA_REFERENCE:
                nested_ref = cast(SchemaReferenceProperty, nested_value_type)
                lines.append(
                    f'{prefix}\tfor (const auto &[nested_key, nested_value] : value_map) {{'
                )
                
                if nested_ref.ref in self.parse_info.recursive_schemas:
                    lines.append(
                        f'{prefix}\t\tauto nested_obj = nested_value->ToJSON(writer);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tauto nested_obj = nested_value.ToJSON(writer);'
                    )
                
                lines.extend([
                    f'{prefix}\t\tvalue_obj.Add(nested_key, nested_obj);',
                    f'{prefix}\t}}'
                ])
            
            lines.extend([
                f'{prefix}\titem_val.Add(key, value_obj);'
            ]
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
                    f'{prefix}item_val.AddString("{prop_name}", item.{prop_name});'
                )
            elif prim_type == 'integer':
                if prim_prop.format == 'int64':
                    lines.append(
                        f'{prefix}item_val.Add("{prop_name}", writer.CreateSignedInteger(item.{prop_name}));'
                    )
                else:
                    lines.append(
                        f'{prefix}item_val.Add("{prop_name}", writer.CreateSignedInteger(item.{prop_name}));'
                    )
            elif prim_type == 'boolean':
                lines.append(
                    f'{prefix}item_val.Add("{prop_name}", writer.CreateBoolean(item.{prop_name}));'
                )
            elif prim_type == 'number':
                lines.append(
                    f'{prefix}item_val.Add("{prop_name}", writer.CreateDouble(item.{prop_name}));'
                )
        
        elif prop_schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, prop_schema)
            
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.extend([
                    f'{prefix}auto {prop_name}_obj = item.{prop_name}->ToJSON(writer);',
                    f'{prefix}item_val.Add("{prop_name}", {prop_name}_obj);'
                ])
            else:
                lines.extend([
                    f'{prefix}auto {prop_name}_obj = item.{prop_name}.ToJSON(writer);',
                    f'{prefix}item_val.Add("{prop_name}", {prop_name}_obj);'
                ])
        
        return lines

    def _serialize_nested_array_item(
        self,
        nested_array: ArrayProperty,
        prefix: str
    ) -> List[str]:
        """Serialize nested array items (array of arrays)"""
        
        lines = [
            f'{prefix}\tauto item_val = writer.CreateArray();',
            f'{prefix}\tfor (const auto &nested_item : item) {{'
        ]
        
        nested_item_type = nested_array.item_type
        
        if nested_item_type.type == Property.Type.PRIMITIVE:
            prim_nested = cast(PrimitiveProperty, nested_item_type)
            if prim_nested.primitive_type == 'string':
                lines.append(
                    f'{prefix}\t\tauto nested_val = writer.CreateString(nested_item);'
                )
            elif prim_nested.primitive_type == 'integer':
                if prim_nested.format == 'int64':
                    lines.append(
                        f'{prefix}\t\tauto nested_val = writer.CreateSignedInteger(nested_item);'
                    )
                else:
                    lines.append(
                        f'{prefix}\t\tauto nested_val = writer.CreateSignedInteger(nested_item);'
                    )
            elif prim_nested.primitive_type == 'boolean':
                lines.append(
                    f'{prefix}\t\tauto nested_val = writer.CreateBoolean(nested_item);'
                )
            elif prim_nested.primitive_type == 'number':
                lines.append(
                    f'{prefix}\t\tauto nested_val = writer.CreateDouble(nested_item);'
                )
        elif nested_item_type.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, nested_item_type)
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.append(
                    f'{prefix}\t\tauto nested_val = nested_item->ToJSON(writer);'
                )
            else:
                lines.append(
                    f'{prefix}\t\tauto nested_val = nested_item.ToJSON(writer);'
                )
        
        lines.extend([
            f'{prefix}\t\titem_val.Append(nested_val);',
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
                f'{prefix}auto {var_name}_val = {var_name}->ToJSON(writer);',
                f'{prefix}obj.Add("{json_name}", {var_name}_val);'
            ]
        else:
            # Normal schema - call ToJSON directly
            return [
                f'{prefix}auto {var_name}_val = {var_name}.ToJSON(writer);',
                f'{prefix}obj.Add("{json_name}", {var_name}_val);'
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
            # Raw JSON value - copy it into the writer
            return [
                f'{prefix}obj.Add("{json_name}", writer.CreateCopy({var_name}));'
            ]
        elif prop.additional_properties:
            # Map type - iterate and add
            lines = [
                f'{prefix}auto {var_name}_obj = writer.CreateObject();',
                f'{prefix}for (const auto &it : {var_name}) {{',
                f'{prefix}\tauto &key = it.first;',
                f'{prefix}\tauto &value = it.second;',
            ]
            
            # Serialize map values based on their type
            add_prop = prop.additional_properties
            if add_prop.type == Property.Type.PRIMITIVE:
                prim_prop = cast(PrimitiveProperty, add_prop)
                if prim_prop.primitive_type == 'string':
                    lines.extend([
                        f'{prefix}\t{var_name}_obj.AddString(key, value);'
                    ]
                    )
                elif prim_prop.primitive_type == 'integer':
                    lines.extend([
                        f'{prefix}\t{var_name}_obj.Add(key, writer.CreateSignedInteger(value));'
                    ]
                    )
                elif prim_prop.primitive_type == 'boolean':
                    lines.extend([
                        f'{prefix}\t{var_name}_obj.Add(key, writer.CreateBoolean(value));'
                    ]
                    )
                elif prim_prop.primitive_type == 'number':
                    lines.extend([
                        f'{prefix}\t{var_name}_obj.Add(key, writer.CreateDouble(value));'
                    ]
                    )
            elif add_prop.type == Property.Type.SCHEMA_REFERENCE:
                schema_ref = cast(SchemaReferenceProperty, add_prop)
                lines.append(
                    f'{prefix}\tauto value_obj = value.ToJSON(writer);'
                )
                lines.extend([
                    f'{prefix}\t{var_name}_obj.Add(key, value_obj);'
                ]
                )
            
            lines.extend([
                f'{prefix}}}',
                f'{prefix}obj.Add("{json_name}", {var_name}_obj);'
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
                lines.extend([
                    "\t\tobj.AddString(key, value);"
                ]
                )
            elif prim_prop.primitive_type == 'integer':
                if prim_prop.format == 'int64':
                    lines.extend([
                        "\t\tobj.Add(key, writer.CreateSignedInteger(value));"
                    ]
                    )
                else:
                    lines.extend([
                        "\t\tobj.Add(key, writer.CreateSignedInteger(value));"
                    ]
                    )
            elif prim_prop.primitive_type == 'boolean':
                lines.extend([
                    "\t\tobj.Add(key, writer.CreateBoolean(value));"
                ]
                )
            elif prim_prop.primitive_type == 'number':
                lines.extend([
                    "\t\tobj.Add(key, writer.CreateDouble(value));"
                ]
                )
        elif add_prop.type == Property.Type.SCHEMA_REFERENCE:
            schema_ref = cast(SchemaReferenceProperty, add_prop)
            if schema_ref.ref in self.parse_info.recursive_schemas:
                lines.extend([
                    "\t\tauto value_obj = value->ToJSON(writer);",
                    "\t\tobj.Add(key, value_obj);"
                ])
            else:
                lines.extend([
                    "\t\tauto value_obj = value.ToJSON(writer);",
                    "\t\tobj.Add(key, value_obj);"
                ])
        elif add_prop.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, add_prop)
            item_property = array_property.item_type
            lines.append("\t\tauto value_obj = writer.CreateArray();")
            lines.append("\t\tfor (const auto &array_item : value) {")
            if item_property.type == Property.Type.PRIMITIVE:
                primitive_item = cast(PrimitiveProperty, item_property)
                if primitive_item.primitive_type == 'string':
                    lines.append(
                        "\t\t\tvalue_obj.AppendString(array_item);"
                    )
                elif primitive_item.primitive_type == 'integer':
                    lines.append("\t\t\tvalue_obj.Append(writer.CreateSignedInteger(array_item));")
                elif primitive_item.primitive_type == 'boolean':
                    lines.append("\t\t\tvalue_obj.Append(writer.CreateBoolean(array_item));")
                elif primitive_item.primitive_type == 'number':
                    lines.append("\t\t\tvalue_obj.Append(writer.CreateDouble(array_item));")
            elif item_property.type == Property.Type.SCHEMA_REFERENCE:
                item_ref = cast(SchemaReferenceProperty, item_property)
                accessor = 'array_item->' if item_ref.ref in self.parse_info.recursive_schemas else 'array_item.'
                lines.append(f"\t\t\tvalue_obj.Append({accessor}ToJSON(writer));")
            else:
                lines.append(
                    '\t\t\tthrow InvalidInputException("Unsupported nested array value in additionalProperties");'
                )
            lines.extend([
                "\t\t}",
                "\t\tobj.Add(key, value_obj);",
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

    with open(os.path.join(OUTPUT_HEADER_DIR, 'json_utils.hpp'), 'w') as f:
        f.write(JSON_UTILS_HEADER_FORMAT.format())

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
