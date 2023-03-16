from setuptools import find_packages, setup

setup(
    name="risingwave",
    version="0.0.1",
    author="RisingWave Labs",
    description="RisingWave Python API",
    url="https://github.com/risingwavelabs/risingwave",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: Apache Software License"
    ],
    python_requires=">=3.10",
)
