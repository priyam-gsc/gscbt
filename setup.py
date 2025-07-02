from setuptools import setup, find_packages

setup(
    name="gscbt",
    version="0.2.2",
    author="Priyam",
    description="Data Pipeline for backtesting",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/priyam-gsc/gscbt",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "requests",
        "dotenv",
        "python_calamine",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    package_data={
        "gscbt": ["config/*.xlsx"]
    },
    include_package_data=True,
)
