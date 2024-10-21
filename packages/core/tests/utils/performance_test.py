from unittest.mock import patch

import pytest

from core.utils.performance import PerformanceTimer


def test_performance_timer_as_context_manager():
    with patch("time.perf_counter", side_effect=[0, 1]):
        with PerformanceTimer("Test") as timer:
            pass

    assert timer.elapsed_time == 1


def test_performance_timer_as_context_manager_without_name():
    with patch("time.perf_counter", side_effect=[0, 1]):
        with PerformanceTimer() as timer:
            pass

    assert timer.elapsed_time == 1


def test_performance_timer_as_decorator():
    @PerformanceTimer("Decorated function")
    def sample_function():
        return "result"

    with patch("time.perf_counter", side_effect=[0, 1]):
        result, execution_time = sample_function()

    assert result == "result"
    assert execution_time == 1


def test_performance_timer_elapsed_time_property():
    timer = PerformanceTimer()
    timer.start_time = 0
    timer.end_time = 1
    assert timer.elapsed_time == 1


def test_performance_timer_print_with_name(capsys):
    with patch("time.perf_counter", side_effect=[0, 1]):
        with PerformanceTimer("Named timer"):
            pass

    captured = capsys.readouterr()
    assert "Named timer executed in 1.000000 seconds" in captured.out


def test_performance_timer_print_without_name(capsys):
    with patch("time.perf_counter", side_effect=[0, 1]):
        with PerformanceTimer():
            pass

    captured = capsys.readouterr()
    assert "Code block executed in 1.000000 seconds" in captured.out


def test_performance_timer_exception_handling():
    with pytest.raises(ValueError):
        with PerformanceTimer("Exception test"):
            raise ValueError("Test exception")


def test_performance_timer_nested():
    with patch("time.perf_counter", side_effect=[0, 1, 2, 3]):
        with PerformanceTimer("Outer") as outer:
            with PerformanceTimer("Inner") as inner:
                pass

    assert outer.elapsed_time == 3
    assert inner.elapsed_time == 1


def test_performance_timer_multiple_calls():
    timer = PerformanceTimer("Multiple calls")

    @timer
    def func1():
        return "result1"

    @timer
    def func2():
        return "result2"

    with patch("time.perf_counter", side_effect=[0, 1, 2, 3]):
        result1, time1 = func1()
        result2, time2 = func2()

    assert result1 == "result1"
    assert result2 == "result2"
    assert time1 == 1
    assert time2 == 1
