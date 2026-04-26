from setuptools import setup, find_packages

setup(
    name="lighthouse",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31",
        "scrapelib>=2.0",
        "beautifulsoup4>=4.12",
        "lxml>=5.0",
        "xmltodict>=0.13",
        "pyyaml>=6.0",
        "python-dateutil>=2.8",
        "pdfplumber>=0.10",
        "sqlalchemy>=2.0",
        "psycopg[binary]>=3.1",
        "alembic>=1.13",
        "fastapi>=0.115",
        "uvicorn>=0.30",
        "jinja2>=3.1",
        "pandas>=2.0",
        "click>=8.1",
        "pytest>=8.0",
        "tenacity>=8.2",
        "ratelimit>=2.2",
    ],
    entry_points={
        "console_scripts": [
            "lh=lighthouse.cli:main",
            "lh-ingest=scripts.ingest:cli",
            "lh-detect=scripts.detect:cli",
            "lh-report=scripts.report:cli",
        ],
    },
    python_requires=">=3.10",
)
