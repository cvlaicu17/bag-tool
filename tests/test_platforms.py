"""Smoke tests for the platform auto-detection logic."""

from __future__ import annotations

import pytest

from bag_tool.platforms import PLATFORMS, auto_detect


def test_detects_prince_from_topics():
    cfg = auto_detect(['/m300/rtk/fix', '/m300/rtk/yaw', '/foo'])
    assert cfg is PLATFORMS['prince']


def test_detects_altair_from_topics():
    cfg = auto_detect(['/pf_geo_loc/fc_local_position', '/imu/data_raw'])
    assert cfg is PLATFORMS['altair']


def test_raises_on_no_match():
    with pytest.raises(ValueError, match='cannot auto-detect'):
        auto_detect(['/imu/data_raw', '/foo'])


def test_raises_on_ambiguous():
    with pytest.raises(ValueError, match='ambiguous'):
        auto_detect(['/m300/rtk/fix', '/pf_geo_loc/fc_local_position'])
