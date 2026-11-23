from fractions import Fraction

from app.dto.enum import FieldKey, FieldType, UnitIOCompatibilityIssue
from app.schemas.pydantic.unit_manifest import FieldDeclaration
from app.services.ucum_conversion import UCUMConversion, UCUMUnits


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

    def check_scalar(
        self, output: FieldDeclaration, input_field: FieldDeclaration
    ) -> UnitIOCompatibilityIssue | None:
        source = FieldType(output[FieldKey.TYPE])
        target = FieldType(input_field[FieldKey.TYPE])
        if target not in self.SCALAR_TARGETS.get(source, ()):
            return UnitIOCompatibilityIssue.TYPE
        if (
            output.get(FieldKey.NULLABLE, False)
            and not input_field.get(FieldKey.NULLABLE, False)
            and target != FieldType.ANY
        ):
            return UnitIOCompatibilityIssue.NULLABLE
        if target in self.NUMERIC_TYPES:
            return self.check_range(output, input_field, source, target)
        return None

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
        return None

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
