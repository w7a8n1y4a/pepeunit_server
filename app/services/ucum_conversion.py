import json
import sys
from dataclasses import dataclass
from fractions import Fraction

from lark import Token, Tree
from lark.exceptions import VisitError
from pint.errors import PintError
from pydantic import TypeAdapter
from ucumvert import (
    InvalidUcumError,
    PintUcumRegistry,
    UcumToPintTransformer,
    get_ucum_parser,
    parse_ucum,
)
from ucumvert.xml_util import get_units_with_full_definition


@dataclass(frozen=True)
class UCUMConversion:
    scale: Fraction = Fraction(1)
    offset: Fraction = Fraction(0)

    def apply(self, value: Fraction) -> Fraction:
        return self.scale * value + self.offset


class UCUMUnits:
    MAX_EXPONENT = 1024

    def __init__(self) -> None:
        self.parser = get_ucum_parser()
        self.registry = PintUcumRegistry(non_int_type=Fraction)
        self.transformer = UcumToPintTransformer(ureg=self.registry)
        self.arbitrary_codes = {
            unit.code_cs
            for unit in get_units_with_full_definition()
            if unit.is_arbitrary
        }

    def conversion(self, source: str, target: str) -> UCUMConversion | None:
        try:
            source_tree = parse_ucum(source, self.parser)
            if source == target:
                return UCUMConversion()
            target_tree = parse_ucum(target, self.parser)
            if not (
                self.supports_conversion(source_tree)
                and self.supports_conversion(target_tree)
            ):
                return None
            return self.affine_conversion(source_tree, target_tree)
        except (
            ArithmeticError,
            InvalidUcumError,
            PintError,
            ValueError,
            VisitError,
        ):
            return None

    def supports_conversion(self, parsed: Tree) -> bool:
        for subtree in parsed.iter_subtrees():
            for item in subtree.children:
                if not isinstance(item, Token):
                    continue
                if item in self.arbitrary_codes:
                    return False
                if (
                    item.type == "EXPONENT"
                    and abs(int(item)) > self.MAX_EXPONENT
                ):
                    return False
        return True

    def affine_conversion(
        self, source: Tree, target: Tree
    ) -> UCUMConversion | None:
        source_unit = self.registry.Quantity(
            self.transformer.transform(source)
        )
        target_unit = self.registry.Quantity(
            self.transformer.transform(target)
        )
        _, source_root = self.registry.get_root_units(source_unit.units)
        _, target_root = self.registry.get_root_units(target_unit.units)
        if source_root != target_root:
            return None

        zero, one = (
            self.registry.Quantity(
                value * source_unit.magnitude, source_unit.units
            )
            .to(target_unit.units)
            .magnitude
            for value in (0, 1)
        )
        if not all(
            isinstance(value, (Fraction, int)) for value in (zero, one)
        ):
            return None
        offset = Fraction(zero, target_unit.magnitude)
        scale = Fraction(one - zero, target_unit.magnitude)
        if scale == 0:
            return None
        return UCUMConversion(scale=scale, offset=offset)

    def write_conversions(self) -> None:
        pairs = TypeAdapter(list[tuple[str, str]]).validate_json(
            sys.stdin.buffer.read()
        )
        result: list[tuple[str, str] | None] = []
        for source, target in pairs:
            conversion = self.conversion(source, target)
            result.append(
                (str(conversion.scale), str(conversion.offset))
                if conversion
                else None
            )
        json.dump(result, sys.stdout)


if __name__ == "__main__":
    UCUMUnits().write_conversions()
