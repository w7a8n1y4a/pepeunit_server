from collections.abc import Iterable
from dataclasses import dataclass
from fractions import Fraction

from app.dto.enum import (
    FieldKey,
    FieldType,
    OutputRouting,
    UnitIOCompatibilityIssue,
)
from app.schemas.pydantic.unit_manifest import (
    FieldDeclaration,
    FieldDeclarations,
)
from app.services.ucum_conversion import UCUMConversion, UCUMUnits


@dataclass(frozen=True)
class UnitIOCompatibilityResult:
    issue: UnitIOCompatibilityIssue | None = None
    path: tuple[str, ...] = ()
    depends_on_value: bool = False

    @property
    def compatible(self) -> bool:
        return self.issue is None


class UnitIOCompatibility:
    NUMERIC_TYPES = (FieldType.INT, FieldType.FLOAT)
    NUMERIC_TARGETS = frozenset(
        (
            FieldType.BOOL,
            FieldType.INT,
            FieldType.FLOAT,
            FieldType.STRING,
            FieldType.ANY,
        )
    )
    SCALAR_TARGETS = {
        FieldType.BOOL: frozenset(
            (FieldType.BOOL, FieldType.INT, FieldType.FLOAT, FieldType.ANY)
        ),
        FieldType.INT: NUMERIC_TARGETS,
        FieldType.FLOAT: NUMERIC_TARGETS,
        FieldType.STRING: frozenset((FieldType.STRING, FieldType.ANY)),
        FieldType.BYTES: frozenset((FieldType.BYTES,)),
    }

    def __init__(self) -> None:
        self.units: UCUMUnits | None = None

    def check_link(
        self, output: FieldDeclaration, input_field: FieldDeclaration
    ) -> UnitIOCompatibilityResult:
        if output.get(FieldKey.ROUTING) != OutputRouting.SELECTED_LINK:
            return self.check(output, input_field)
        if FieldKey.VARIANTS in output:
            return self.combine(
                self.check_routed_data(
                    fields, input_field, (FieldKey.VARIANTS.value, name)
                )
                for name, fields in output[FieldKey.VARIANTS].items()
            )
        return self.check_routed_data(
            output.get(FieldKey.FIELDS, {}), input_field, ()
        )

    def check_routed_data(
        self,
        fields: FieldDeclarations,
        input_field: FieldDeclaration,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        data = fields.get(FieldKey.DATA)
        data_path = (*path, FieldKey.DATA.value)
        if data is None or data.get(FieldKey.OPTIONAL, False):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.REQUIRED_FIELD, data_path
            )
        return self.check(data, input_field, data_path)

    def check(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        path: tuple[str, ...] = (),
    ) -> UnitIOCompatibilityResult:
        source = FieldType(output[FieldKey.TYPE])
        target = FieldType(input_field[FieldKey.TYPE])
        if FieldType.ANY in (source, target):
            if FieldType.BYTES in (source, target):
                return UnitIOCompatibilityResult(
                    UnitIOCompatibilityIssue.TYPE, path
                )
            depends_on_value = (
                source == FieldType.ANY and target != FieldType.ANY
            )
            return UnitIOCompatibilityResult(
                path=path if depends_on_value else (),
                depends_on_value=depends_on_value,
            )
        if source != target or source not in (
            FieldType.ARRAY,
            FieldType.OBJECT,
        ):
            issue = self.check_scalar(output, input_field)
            return (
                UnitIOCompatibilityResult(issue, path)
                if issue
                else UnitIOCompatibilityResult()
            )
        if self.nullable_mismatch(output, input_field):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.NULLABLE, path
            )
        if source == FieldType.ARRAY:
            return self.check_array(output, input_field, path)
        return self.check_object(output, input_field, path)

    def check_array(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        if output.get(FieldKey.MIN_ITEMS, 0) < input_field.get(
            FieldKey.MIN_ITEMS, 0
        ):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.LENGTH,
                (*path, FieldKey.MIN_ITEMS.value),
            )
        maximum = output.get(FieldKey.MAX_ITEMS)
        if FieldKey.MAX_ITEMS in input_field and (
            maximum is None or maximum > input_field[FieldKey.MAX_ITEMS]
        ):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.LENGTH,
                (*path, FieldKey.MAX_ITEMS.value),
            )
        if maximum == 0:
            return UnitIOCompatibilityResult()
        return self.check(
            output[FieldKey.ITEMS],
            input_field[FieldKey.ITEMS],
            (*path, FieldKey.ITEMS.value),
        )

    def check_object(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        if FieldKey.VARIANTS in output:
            return self.combine(
                self.check_variant(output, input_field, action, path)
                for action in output[FieldKey.VARIANTS]
            )
        if FieldKey.VARIANTS in input_field:
            return self.check_variant_selector(output, input_field, path)
        return self.check_object_fields(
            output.get(FieldKey.FIELDS, {}),
            input_field.get(FieldKey.FIELDS, {}),
            output.get(FieldKey.EXTRA, False),
            input_field.get(FieldKey.EXTRA, False),
            path,
        )

    def check_variant_selector(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        selector_path = (*path, FieldKey.ACTION.value)
        selector = output.get(FieldKey.FIELDS, {}).get(FieldKey.ACTION)
        if selector is None or selector.get(FieldKey.OPTIONAL, False):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.REQUIRED_FIELD, selector_path
            )
        if selector[FieldKey.TYPE] == FieldType.ANY:
            compatible = any(
                self.check_variant(
                    output, input_field, action, path
                ).compatible
                for action in input_field[FieldKey.VARIANTS]
            )
            return UnitIOCompatibilityResult(
                issue=None if compatible else UnitIOCompatibilityIssue.VARIANT,
                path=selector_path,
                depends_on_value=compatible,
            )
        if selector[FieldKey.TYPE] != FieldType.STRING:
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.TYPE, selector_path
            )
        if selector.get(FieldKey.NULLABLE, False):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.NULLABLE, selector_path
            )
        if FieldKey.ENUM not in selector:
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.VARIANT, selector_path
            )
        return self.combine(
            self.check_variant(output, input_field, action, path)
            for action in selector[FieldKey.ENUM]
            if isinstance(action, str)
        )

    def check_variant(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        action: str,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        variant_path = (*path, FieldKey.VARIANTS.value, action)
        selector: FieldDeclaration = {
            FieldKey.TYPE: FieldType.STRING,
            FieldKey.ENUM: [action],
        }
        source_fields = (
            output[FieldKey.VARIANTS][action]
            if FieldKey.VARIANTS in output
            else output.get(FieldKey.FIELDS, {})
        ) | {FieldKey.ACTION.value: selector}
        target_fields = input_field.get(FieldKey.FIELDS, {})
        if FieldKey.VARIANTS in input_field:
            variant = input_field[FieldKey.VARIANTS].get(action)
            if variant is None:
                return UnitIOCompatibilityResult(
                    UnitIOCompatibilityIssue.VARIANT, variant_path
                )
            target_fields = variant | {FieldKey.ACTION.value: selector}
        return self.check_object_fields(
            source_fields,
            target_fields,
            output.get(FieldKey.EXTRA, False),
            input_field.get(FieldKey.EXTRA, False),
            variant_path,
        )

    def check_object_fields(
        self,
        output_fields: FieldDeclarations,
        input_fields: FieldDeclarations,
        output_extra: bool,
        input_extra: bool,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        if not input_extra:
            for name in output_fields:
                if name not in input_fields:
                    return UnitIOCompatibilityResult(
                        UnitIOCompatibilityIssue.EXTRA_FIELD, (*path, name)
                    )
            if output_extra:
                return UnitIOCompatibilityResult(
                    UnitIOCompatibilityIssue.EXTRA_FIELD,
                    (*path, FieldKey.EXTRA.value),
                )
        return self.combine(
            self.check_object_field(
                output_fields.get(name),
                input_field,
                output_extra,
                (*path, name),
            )
            for name, input_field in input_fields.items()
        )

    def check_object_field(
        self,
        output: FieldDeclaration | None,
        input_field: FieldDeclaration,
        output_extra: bool,
        path: tuple[str, ...],
    ) -> UnitIOCompatibilityResult:
        if not input_field.get(FieldKey.OPTIONAL, False) and (
            output is None or output.get(FieldKey.OPTIONAL, False)
        ):
            return UnitIOCompatibilityResult(
                UnitIOCompatibilityIssue.REQUIRED_FIELD, path
            )
        if output is None:
            if not output_extra:
                return UnitIOCompatibilityResult()
            output = {FieldKey.TYPE: FieldType.ANY}
        return self.check(output, input_field, path)

    @staticmethod
    def combine(
        results: Iterable[UnitIOCompatibilityResult],
    ) -> UnitIOCompatibilityResult:
        result = UnitIOCompatibilityResult()
        for candidate in results:
            if not candidate.compatible:
                return candidate
            if candidate.depends_on_value and not result.depends_on_value:
                result = candidate
        return result

    @staticmethod
    def nullable_mismatch(
        output: FieldDeclaration, input_field: FieldDeclaration
    ) -> bool:
        return output.get(FieldKey.NULLABLE, False) and not input_field.get(
            FieldKey.NULLABLE, False
        )

    def check_scalar(
        self, output: FieldDeclaration, input_field: FieldDeclaration
    ) -> UnitIOCompatibilityIssue | None:
        source = FieldType(output[FieldKey.TYPE])
        target = FieldType(input_field[FieldKey.TYPE])
        if target not in self.SCALAR_TARGETS.get(source, ()):
            return UnitIOCompatibilityIssue.TYPE
        if target != FieldType.ANY and self.nullable_mismatch(
            output, input_field
        ):
            return UnitIOCompatibilityIssue.NULLABLE
        if target in self.NUMERIC_TYPES:
            return self.check_range(output, input_field, source, target)
        if target == FieldType.BOOL:
            return self.check_bool_enum(output, input_field, source)
        if (
            source == target == FieldType.STRING
            and FieldKey.ENUM in input_field
            and (
                FieldKey.ENUM not in output
                or any(
                    value is not None
                    and value not in input_field[FieldKey.ENUM]
                    for value in output[FieldKey.ENUM]
                )
            )
        ):
            return UnitIOCompatibilityIssue.ENUM
        return None

    def check_bool_enum(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        source: FieldType,
    ) -> UnitIOCompatibilityIssue | None:
        if FieldKey.ENUM not in input_field:
            return None
        if self.bool_values(output, source).issubset(
            input_field[FieldKey.ENUM]
        ):
            return None
        return UnitIOCompatibilityIssue.ENUM

    def bool_values(
        self, output: FieldDeclaration, source: FieldType
    ) -> set[bool]:
        if FieldKey.ENUM in output:
            return {
                bool(value)
                for value in output[FieldKey.ENUM]
                if value is not None
            }
        if source == FieldType.BOOL:
            return {False, True}
        low, high = self.output_bounds(output, source)
        values = set()
        if low <= 0 <= high:
            values.add(False)
        if low != 0 or high != 0:
            values.add(True)
        return values

    def check_range(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        source: FieldType,
        target: FieldType,
    ) -> UnitIOCompatibilityIssue | None:
        conversion = self.numeric_conversion(output, input_field, source)
        if conversion is None:
            return UnitIOCompatibilityIssue.UNIT
        bounds = self.output_bounds(output, source)
        if bounds is None:
            return None
        low, high = bounds
        if conversion.scale < 0:
            low, high = high, low

        lower = self.convert_bound(low, conversion, target)
        upper = self.convert_bound(high, conversion, target)
        if FieldKey.MIN in input_field and (
            lower is None
            or lower < self.as_fraction(input_field[FieldKey.MIN])
        ):
            return UnitIOCompatibilityIssue.RANGE
        if FieldKey.MAX in input_field and (
            upper is None
            or upper > self.as_fraction(input_field[FieldKey.MAX])
        ):
            return UnitIOCompatibilityIssue.RANGE
        if FieldKey.ENUM in input_field and not self.numeric_enum_covers(
            output, input_field, source, target, conversion, lower, upper
        ):
            return UnitIOCompatibilityIssue.ENUM
        return None

    def numeric_enum_covers(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        source: FieldType,
        target: FieldType,
        conversion: UCUMConversion,
        lower: Fraction | int,
        upper: Fraction | int,
    ) -> bool:
        allowed = {
            self.as_fraction(value)
            for value in input_field[FieldKey.ENUM]
            if value is not None
        }
        if FieldKey.ENUM in output:
            return all(
                self.convert_bound(self.as_fraction(value), conversion, target)
                in allowed
                for value in output[FieldKey.ENUM]
                if value is not None
            )
        if source == FieldType.BOOL:
            return lower in allowed and upper in allowed
        if lower == upper:
            return lower in allowed
        if target == FieldType.INT:
            covered = sum(lower <= value <= upper for value in allowed)
            if covered == upper - lower + 1:
                return True
        if source == FieldType.INT:
            start, end = output[FieldKey.MIN], output[FieldKey.MAX]
            if end - start + 1 <= len(allowed):
                return all(
                    self.convert_bound(Fraction(value), conversion, target)
                    in allowed
                    for value in range(start, end + 1)
                )
        return False

    def numeric_conversion(
        self,
        output: FieldDeclaration,
        input_field: FieldDeclaration,
        source: FieldType,
    ) -> UCUMConversion | None:
        source_unit = output.get(FieldKey.UNIT)
        target_unit = input_field.get(FieldKey.UNIT)
        if source == FieldType.BOOL:
            source_unit = target_unit
        if source_unit is None and target_unit is None:
            return UCUMConversion()
        if source_unit is None or target_unit is None:
            return None
        if self.units is None:
            self.units = UCUMUnits()
        return self.units.conversion(source_unit, target_unit)

    @classmethod
    def output_bounds(
        cls, field: FieldDeclaration, kind: FieldType
    ) -> tuple[Fraction | None, Fraction | None] | None:
        if FieldKey.ENUM in field:
            values = [
                cls.as_fraction(value)
                for value in field[FieldKey.ENUM]
                if isinstance(value, (int, float))
            ]
            return (min(values), max(values)) if values else None
        if kind == FieldType.BOOL:
            return Fraction(0), Fraction(1)
        minimum = field.get(FieldKey.MIN)
        maximum = field.get(FieldKey.MAX)
        return (
            cls.as_fraction(minimum) if minimum is not None else None,
            cls.as_fraction(maximum) if maximum is not None else None,
        )

    @staticmethod
    def as_fraction(value: int | float) -> Fraction:
        return Fraction(value if isinstance(value, int) else str(value))

    @staticmethod
    def convert_bound(
        value: Fraction | None, conversion: UCUMConversion, target: FieldType
    ) -> Fraction | int | None:
        if value is None:
            return None
        converted = conversion.apply(value)
        if target == FieldType.INT:
            half = Fraction(1, 2)
            return int(
                converted + half if converted >= 0 else converted - half
            )
        return converted
