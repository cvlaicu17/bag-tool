"""Unit tests for Project AirSim simulation quality checks."""

from __future__ import annotations

import pytest

from bag_tool.sim_check import _grade, _topic_stats


def test_topic_stats_reports_clean_400hz_cadence():
    timestamps = [1_000_000_000 + i * 2_500_000 for i in range(401)]

    result = _topic_stats(timestamps)

    assert result["count"] == 401
    assert result["rate_hz"] == pytest.approx(400.0)
    assert result["median_ms"] == pytest.approx(2.5)
    assert result["jitter_cv"] == pytest.approx(0.0)
    assert result["gaps"] == 0
    assert result["non_monotonic"] == 0


def test_topic_stats_detects_gap_and_non_monotonic_timestamp():
    timestamps = [0, 2_500_000, 5_500_000, 45_500_000, 45_000_000]

    result = _topic_stats(timestamps)

    assert result["gaps"] == 1
    assert result["non_monotonic"] == 1


@pytest.mark.parametrize(
    ("value", "reference", "tiny", "expected"),
    [
        (1.0, 1.0, 0.02, "ok"),
        (1.4, 1.0, 0.02, "warn"),
        (2.0, 1.0, 0.02, "fail"),
        (0.001, 0.002, 0.02, "ok"),
    ],
)
def test_vibration_grade(value, reference, tiny, expected):
    assert _grade(value, reference, tiny) == expected
