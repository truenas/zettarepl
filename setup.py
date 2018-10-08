from setuptools import find_packages, setup


install_requires = [
    "coloredlogs",
    "croniter",
    "isodate",
    "jsonschema",
    "paramiko",
    "python-dateutil",
    "pytz",
    "pyyaml",
]


setup(
    name="zettarepl",
    description="zettarepl is a cross-platform ZFS replication solution",
    packages=find_packages(),
    package_data={
        "zettarepl.definition.schema": "*.yaml",
        "zettarepl.zcp": "*.lua",
    },
    include_package_data=True,
    license="BSD",
    platforms="any",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Archiving :: Mirroring",
    ],
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "zettarepl = zettarepl.main:main",
        ],
    },
)
