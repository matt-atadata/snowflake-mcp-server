#!/usr/bin/env python3
"""
Setup script for Snowflake MCP Server
"""

from setuptools import setup, find_packages

setup(
    name="snowflake-mcp-server",
    version="0.1.0",
    description="A Model Context Protocol server for Snowflake",
    author="Matt A",
    author_email="matt@atadataco.com",
    packages=find_packages(),
    install_requires=[
        "mcp",
        "snowflake-connector-python>=3.0.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "snowflake-mcp-server=server:main",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
