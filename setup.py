import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rangers-utils",
    version="0.9.0",
    author="Sergei Osminin",
    author_email="murgesku@gmail.com",
    description="Space Rangers HD game file format tools",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/murgesku/rangers-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)