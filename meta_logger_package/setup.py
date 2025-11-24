from setuptools import setup, find_packages

setup(
    name="meta_logger",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "s3fs"
    ],
    python_requires=">=3.8"
)

