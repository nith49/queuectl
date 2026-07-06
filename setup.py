from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    description="A CLI-based background job queue system",
    packages=find_packages(),
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "queuectl=queuectl.cli:main",
        ],
    },
)
