import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kHilltopConnector",
    version="0.0.1",
    author="Karunakar",
    author_email="karunakar.kintada@gmail.com",
    description="Python package to interact with Hilltop data over internet",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hawkes-bay-rc/hydroSOE",
    project_urls={
        "Bug Tracker": "https://github.com/hawkes-bay-rc/hydroSOE/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU GPLv3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    #data_files=[('data',['data/openDataLinks.json'])],
    packages=setuptools.find_packages(where="src"), #+ ['config'],
    python_requires=">=3.6",
)