from setuptools import setup

setup(
    name="datavertex",
    version="0.01",
    description="Framework that streamlines python notebook usage for exploration, data pipelines and science experiments.",
    author="Stanislau Rassolenka",
    url="https://github.com/datavertex/datavertex",
    license="AGPL-3.0 license",    
    packages=["datavertex"],
    install_requires=['papermill', 'jsonlines', 'datetime_truncate', 'uritemplate']
)