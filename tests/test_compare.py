"""The comparison must catch real differences and ignore only the allowed ones."""

import pyarrow as pa
from compare import Tolerance, compare_tables

TOL = Tolerance(rel=1e-9)


def table(**columns):
    return pa.table(columns)


def test_float_summation_noise_is_tolerated():
    left = table(k=[1, 2], v=[2496.0, 1.5])
    right = table(k=[1, 2], v=[2495.9999999999995, 1.5])
    assert compare_tables(left, right, ordered=True, tol=TOL) == []


def test_real_value_difference_is_reported():
    left = table(k=[1, 2], v=[10.0, 20.0])
    right = table(k=[1, 2], v=[10.0, 20.5])
    assert compare_tables(left, right, ordered=True, tol=TOL)


def test_unordered_comparison_ignores_row_order():
    left = table(k=[1, 2, 3], s=["a", "b", "c"])
    right = table(k=[3, 1, 2], s=["c", "a", "b"])
    assert compare_tables(left, right, ordered=False, tol=TOL) == []


def test_ordered_comparison_catches_row_order():
    left = table(k=[1, 2, 3])
    right = table(k=[3, 2, 1])
    assert compare_tables(left, right, ordered=True, tol=TOL)


def test_duplicate_rows_are_caught():
    left = table(k=[1, 2])
    right = table(k=[1, 2, 2])
    assert compare_tables(left, right, ordered=False, tol=TOL)


def test_column_type_difference_is_reported():
    left = table(k=pa.array([1, 2], pa.int64()))
    right = table(k=pa.array([1, 2], pa.int32()))
    assert compare_tables(left, right, ordered=True, tol=TOL)


def test_large_and_small_string_variants_are_equal():
    left = table(s=pa.array(["x", None], pa.large_string()))
    right = table(s=pa.array(["x", None], pa.string()))
    assert compare_tables(left, right, ordered=True, tol=TOL) == []


def test_nested_float_lists_use_tolerance():
    left = table(e=pa.array([[0.1, 0.2]], pa.list_(pa.float32())))
    right = table(e=pa.array([[0.1, 0.2000001]], pa.list_(pa.float32())))
    assert compare_tables(left, right, ordered=True, tol=Tolerance(rel=1e-5)) == []
    assert compare_tables(left, right, ordered=True, tol=TOL)


def test_nan_equals_nan_and_null_differs_from_value():
    assert compare_tables(table(v=[float("nan")]), table(v=[float("nan")]), ordered=True, tol=TOL) == []
    assert compare_tables(table(v=[None, 1.0]), table(v=[1.0, 1.0]), ordered=True, tol=TOL)
