import pytest

from gscbt.utils import (
    Interval,
    MonthMap,
)


### Interval
## Interval.second_to_str

@pytest.mark.parametrize("interval, res", [
    (1, "1s"), (15, "15s"), (59, "59s"),
    (60, "1m"), (3540, "59m"),
    (3600, "1h"), (7200, "2h"),
    (86400, "1d"),
])
def test_Interval_second_to_str(interval, res):
    assert Interval.second_to_str(interval) == res

@pytest.mark.parametrize("interval, e_type", [
    (-1, ValueError),
])
def test_exception_Interval_second_to_str(interval, e_type):
    with pytest.raises(e_type):
        Interval.second_to_str(interval)


## Interval.str_to_second

@pytest.mark.parametrize("interval, res", [
    ("1s", 1), ("15s", 15), ("59s", 59), ("60s", 60),
    ("1m", 60), ("59m", 3540), 
    ("1h", 3600), ("2h", 7200),
    ("1d", 86400),
])
def test_Interval_str_to_second(interval, res):
    assert Interval.str_to_second(interval) == res

@pytest.mark.parametrize("interval, e_type", [
    ("60k", ValueError),
])
def test_exception_Interval_str_to_second(interval, e_type):
    with pytest.raises(e_type):
        Interval.str_to_second(interval)


### MonthMap
## MonthMap.month

@pytest.mark.parametrize("month, res", [
    # param_type : int
    (1, "F"), (2, "G"), (3, "H"), (4, "J"), (5, "K"), (6, "M"),
    (7, "N"), (8, "Q"), (9, "U"), (10, "V"), (11, "X"), (12, "Z"),

    # param_type : str
    ("F", 1), ("G", 2), ("H", 3), ("J", 4), ("K", 5), ("M", 6),
    ("N", 7), ("Q", 8), ("U", 9), ("V", 10), ("X", 11), ("Z", 12),
])
def test_MonthMap_month(month, res):
    assert MonthMap.month(month) == res

@pytest.mark.parametrize("month, e_type", [
    (-1, ValueError), (13, ValueError),
    ("A", ValueError), ("R", ValueError),
])
def test_exception_MonthMap_month(month, e_type):
    with pytest.raises(e_type):
        MonthMap.month(month)


## MonthMap.min
@pytest.mark.parametrize("month_1, month_2, res", [
    ("F", "J", "F"), ("Q", "J", "J"), ("U", "V", "U"),  
    ("Z", "Z", "Z"),
])
def test_exception_MonthMap_min(month_1, month_2, res):
    assert MonthMap.min(month_1, month_2) == res

@pytest.mark.parametrize("month_1, month_2, e_type", [
    ("1", "F", ValueError), ("X", 1, ValueError),
    ("Z", 12, ValueError), ("R", "", ValueError),
])
def test_exception_MonthMap_min(month_1, month_2, e_type):
    with pytest.raises(e_type):
        MonthMap.min(month_1, month_2)