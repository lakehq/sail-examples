"""Compare two Arrow tables produced by different engines.

Rules:
- Column names must match, in order.
- Column types must match after folding the large/small Arrow variants
  (large_string and string, large_binary and binary, large_list and list),
  which carry the same values.
- Rows are compared positionally when the example declares `ordered: true`,
  otherwise as multisets: both sides are sorted by a canonical key first.
- Floats compare with a relative and an absolute tolerance; NaN equals NaN.
"""

import math
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class Tolerance:
    rel: float = 1e-9
    abs: float = 1e-12


def normalize_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_large_binary(arrow_type):
        return "binary"
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        return f"list<{normalize_type(arrow_type.value_type)}>"
    if pa.types.is_fixed_size_list(arrow_type):
        return f"fixed_size_list<{normalize_type(arrow_type.value_type)}>[{arrow_type.list_size}]"
    if pa.types.is_struct(arrow_type):
        fields = ", ".join(f"{f.name}: {normalize_type(f.type)}" for f in arrow_type)
        return f"struct<{fields}>"
    if pa.types.is_map(arrow_type):
        return f"map<{normalize_type(arrow_type.key_type)}, {normalize_type(arrow_type.item_type)}>"
    return str(arrow_type)


def _sort_key(value, digits: int):
    """A total-order key for nested Python values, floats rounded to `digits` significant digits."""
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, value)
    if isinstance(value, float):
        if math.isnan(value):
            return (2, math.inf)
        return (2, float(f"{value:.{digits}g}"))
    if isinstance(value, int):
        return (2, value)
    if isinstance(value, (str, bytes)):
        return (3, value if isinstance(value, str) else value.hex())
    if isinstance(value, dict):
        return (4, tuple((k, _sort_key(v, digits)) for k, v in sorted(value.items())))
    if isinstance(value, (list, tuple)):
        return (5, tuple(_sort_key(v, digits) for v in value))
    return (6, str(value))


def values_equal(left, right, tol: Tolerance) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, float) or isinstance(right, float):
        if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
            return False
        if math.isnan(left) or math.isnan(right):
            return math.isnan(left) and math.isnan(right)
        return math.isclose(left, right, rel_tol=tol.rel, abs_tol=tol.abs)
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(values_equal(left[k], right[k], tol) for k in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(values_equal(a, b, tol) for a, b in zip(left, right))
    return left == right


def compare_tables(left: pa.Table, right: pa.Table, *, ordered: bool, tol: Tolerance) -> list[str]:
    """Return a list of differences; an empty list means the tables match."""
    if left.column_names != right.column_names:
        return [f"columns differ: {left.column_names} vs {right.column_names}"]
    problems = []
    for name in left.column_names:
        lt, rt = normalize_type(left.schema.field(name).type), normalize_type(right.schema.field(name).type)
        if lt != rt:
            problems.append(f"column {name!r} type differs: {lt} vs {rt}")
    if left.num_rows != right.num_rows:
        problems.append(f"row count differs: {left.num_rows} vs {right.num_rows}")
        return problems

    left_rows = [tuple(row.values()) for row in left.to_pylist()]
    right_rows = [tuple(row.values()) for row in right.to_pylist()]
    if not ordered:
        digits = max(0, int(-math.log10(max(tol.rel, tol.abs, 1e-15))) - 2)
        left_rows.sort(key=lambda row: _sort_key(row, digits))
        right_rows.sort(key=lambda row: _sort_key(row, digits))
    mismatches = [i for i, (a, b) in enumerate(zip(left_rows, right_rows)) if not values_equal(a, b, tol)]
    for i in mismatches[:5]:
        problems.append(f"row {i} differs: {left_rows[i]!r} vs {right_rows[i]!r}")
    if len(mismatches) > 5:
        problems.append(f"... {len(mismatches) - 5} more differing rows")
    return problems
