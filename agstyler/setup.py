from setuptools import setup, find_packages

setup(
    name="agstyler",               # Name of the package
    version="0.1.0",               # Version of the package
    packages=find_packages(),      # Automatically discover all modules in the package
    install_requires=['streamlit-aggrid'],           
    description="Formats grids in Streamlit apps",
    author="Reid Bongard; (Modified from package created by Nikolai Riabykh)",
)