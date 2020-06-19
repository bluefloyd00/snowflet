import io
import versioneer
import setuptools


def read(*filenames, **kwargs):
    encoding = kwargs.get("encoding", "utf-8")
    sep = kwargs.get("sep", "\n")
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read("README.md")

setuptools.setup(
    name="snowflet",
    version=versioneer.get_version(),
    author="Simone Fiorentini",
    author_email="simone.fiorentini@gmail.com",
    description="Data engineering & Data science Pipeline Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bluefloyd00/snowflet",
    test_suite="snowflet.tests",
    packages=setuptools.find_packages(),
    install_requires=[
        'wheel',
        'PyYAML==5.3.1',
        'snowflake-sqlalchemy',
        'numpy',
        'pandas',
        'nltk',
        'versioneer'
    ],
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
