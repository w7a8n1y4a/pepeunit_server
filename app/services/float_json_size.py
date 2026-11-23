import struct
import sys
from decimal import Decimal
from fractions import Fraction
from math import ceil, floor, inf, nextafter, ulp


class FloatJsonSize:
    FRACTION_BITS = sys.float_info.mant_dig - 1
    MAX_SIGNIFICANT_DIGITS = sys.float_info.dig + 2
    MAX_POSITIVE_CHARS = 23
    DIRECT_MEASUREMENT_LIMIT = 16
    FIXED_EXPONENTS = range(-4, 16)

    @classmethod
    def maximum(cls, minimum: int | float, maximum: int | float) -> int | None:
        positive = cls.positive_maximum(minimum, maximum)
        negative = cls.positive_maximum(-maximum, -minimum)
        sizes = [len(repr(-0.0))] if minimum <= 0 <= maximum else []
        if positive is not None:
            sizes.append(positive)
        if negative is not None:
            sizes.append(negative + 1)
        return max(sizes, default=None)

    @classmethod
    def positive_maximum(
        cls, minimum: int | float, maximum: int | float
    ) -> int | None:
        minimum = max(minimum, nextafter(0.0, inf))
        maximum = min(maximum, sys.float_info.max)
        if minimum > maximum:
            return None

        first, last = float(minimum), float(maximum)
        if first < minimum:
            first = nextafter(first, inf)
        if last > maximum:
            last = nextafter(last, 0.0)
        start, stop = cls.bits(first), cls.bits(last)
        if start > stop:
            return None

        largest = 0
        while start <= stop:
            value = cls.from_bits(start)
            exponent = Decimal(repr(value)).adjusted()
            next_decimal = cls.bits(float(f"1e{exponent + 1}"))
            next_binary = (
                (start >> cls.FRACTION_BITS) + 1
            ) << cls.FRACTION_BITS
            end = min(stop, next_decimal - 1, next_binary - 1)
            on_boundary = (
                start == cls.bits(float(f"1e{exponent}"))
                or start % (1 << cls.FRACTION_BITS) == 0
            )
            if on_boundary:
                largest = max(largest, len(repr(value)))
                start += 1
                continue
            if end - start < cls.DIRECT_MEASUREMENT_LIMIT:
                measured = (
                    len(repr(cls.from_bits(item)))
                    for item in range(start, end + 1)
                )
                largest = max(largest, *measured)
            else:
                digits = cls.maximum_digits(start, end, exponent)
                largest = max(largest, cls.repr_length(exponent, digits))
            if largest == cls.MAX_POSITIVE_CHARS:
                return largest
            start = end + 1
        return largest

    @classmethod
    def maximum_digits(cls, start: int, end: int, exponent: int) -> int:
        """Count which floats a decimal grid can round back to."""
        first, last = cls.from_bits(start), cls.from_bits(end)
        spacing = Fraction(ulp(first))
        left_edge = (Fraction(nextafter(first, 0.0)) + Fraction(first)) / 2
        right_edge = Fraction(last) + spacing / 2
        population = end - start + 1

        for digits in range(cls.MAX_SIGNIFICANT_DIGITS - 1, 0, -1):
            grid = Fraction(10) ** (exponent - digits + 1)
            if grid <= spacing:
                continue
            left, right = left_edge / grid, right_edge / grid
            first_point = ceil(left) + (left.denominator == 1 and start % 2)
            last_point = floor(right) - (right.denominator == 1 and end % 2)
            if last_point - first_point + 1 < population:
                return digits + 1
        return 1

    @classmethod
    def repr_length(cls, exponent: int, digits: int) -> int:
        if exponent not in cls.FIXED_EXPONENTS:
            return digits + (digits > 1) + 2 + max(2, len(str(abs(exponent))))
        if exponent < 0:
            return digits + 1 - exponent
        return max(exponent + 3, digits + 1)

    @staticmethod
    def bits(value: float) -> int:
        return int.from_bytes(struct.pack(">d", value), "big")

    @staticmethod
    def from_bits(bits: int) -> float:
        return struct.unpack(">d", bits.to_bytes(8, "big"))[0]
