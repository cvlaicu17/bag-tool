"""Unit tests for Project AirSim simulation quality checks."""

from __future__ import annotations

import pytest

from bag_tool.sim_check import _grade
from bag_tool.vio_check import analyze_topic


def test_shared_vio_cadence_analysis_reports_clean_400hz_cadence():
    timestamps = [1_000_000_000 + i * 2_500_000 for i in range(401)]

    result = analyze_topic("/imu/data_raw", timestamps, timestamps, 400.0,
                           rate_tolerance=0.02, strict_gaps=True)

    assert result["msg_count"] == 401
    assert result["mean_hz"] == pytest.approx(400.0)
    assert result["log_interval"]["p50"] == pytest.approx(2_500_000)
    assert result["jitter_cv"] == pytest.approx(0.0)
    assert result["gaps"] == []
    assert result["non_monotonic_log"] == 0
    assert result["pass"]


def test_shared_vio_cadence_analysis_detects_gap_and_non_monotonic_timestamp():
    timestamps = [0, 2_500_000, 5_500_000, 45_500_000, 45_000_000]

    result = analyze_topic("/imu/data_raw", timestamps, timestamps, 400.0,
                           rate_tolerance=0.02, strict_gaps=True)

    assert len(result["gaps"]) == 1
    assert result["non_monotonic_log"] == 1
    assert not result["pass"]


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
