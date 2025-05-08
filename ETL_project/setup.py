from setuptools import find_packages, setup

setup(
    name="ETL_project",
    packages=find_packages(exclude=["ETL_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
