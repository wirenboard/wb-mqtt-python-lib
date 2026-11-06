#!/usr/bin/env python3

from setuptools import find_packages, setup


def get_version_from_changelog() -> str:
    """
    Version from the first line of debian/changelog without the part after ~. PEP 440 allows no ~,
    and after it come the alpha mark and the suffix CI adds on dev branches.
    """
    with open("debian/changelog", "r", encoding="utf-8") as changelog:
        return changelog.readline().split()[1][1:-1].split("~")[0]


setup(
    name="wb-mqtt-lib",
    version=get_version_from_changelog(),
    maintainer="Wiren Board Team",
    maintainer_email="info@wirenboard.com",
    description="Python library for MQTT and the Wiren Board convention",
    url="https://github.com/wirenboard/wb-mqtt-python-lib",
    license="MIT",
    packages=find_packages(include=["wb_mqtt_lib", "wb_mqtt_lib.*"]),
)
