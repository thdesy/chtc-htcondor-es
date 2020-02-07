from pathlib import Path

from setuptools import setup

THIS_DIR = Path(__file__).parent

setup(
    name="chtc-htcondor-es",
    version="0.1.0",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=["htcondor_es"],
    entry_points={"console_scripts": ["spider = htcondor_es.spider:main"]},
    install_requires=Path("requirements.txt").read_text().splitlines(),
)
