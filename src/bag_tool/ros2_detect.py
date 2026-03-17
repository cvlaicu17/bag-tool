"""Detect the appropriate ROS2 distribution for the current Ubuntu release."""

from __future__ import annotations
import platform

from rosbags.typesys import Stores


# Ubuntu VERSION_ID → (distro name, Stores enum)
_UBUNTU_TO_ROS2: dict[str, tuple[str, Stores]] = {
    "20.04": ("foxy",   Stores.ROS2_FOXY),
    "22.04": ("humble", Stores.ROS2_HUMBLE),
    "24.04": ("jazzy",  Stores.ROS2_JAZZY),
}

_ROLLING_NAME  = "rolling"
_ROLLING_STORE = Stores.LATEST


def _read_os_release() -> dict[str, str]:
    try:
        return platform.freedesktop_os_release()
    except OSError:
        return {}


def detect_ros2_distro() -> str:
    """Return the recommended ROS2 distribution name for the current system."""
    info = _read_os_release()
    if info.get("ID", "").lower() != "ubuntu":
        return _ROLLING_NAME
    entry = _UBUNTU_TO_ROS2.get(info.get("VERSION_ID", ""))
    return entry[0] if entry else _ROLLING_NAME


def detect_stores_enum() -> Stores:
    """Return the rosbags Stores enum value matching the current system's ROS2 distro."""
    info = _read_os_release()
    if info.get("ID", "").lower() != "ubuntu":
        return _ROLLING_STORE
    entry = _UBUNTU_TO_ROS2.get(info.get("VERSION_ID", ""))
    return entry[1] if entry else _ROLLING_STORE


def detect_ros2_distro_verbose() -> dict[str, str]:
    """Return detection details: ubuntu_version, ubuntu_codename, ros2_distro."""
    info = _read_os_release()
    return {
        "ubuntu_version":  info.get("VERSION_ID", "unknown"),
        "ubuntu_codename": info.get("VERSION_CODENAME", "unknown"),
        "ros2_distro":     detect_ros2_distro(),
    }
