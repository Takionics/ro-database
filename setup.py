import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ibm_db_conns-takionics3261",
    version="0.0.1",
    author="Islam Elkadi",
    author_email="islam.elkadi@gmail.com",
    description="An python library for IBM Cloud database connectors",
    long_description="An python library for IBM Cloud database connectors",
    long_description_content_type="text/markdown",
    url="https://github.com/Takionics/restaurant-forecaster/tree/master/database-container",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)