from setuptools import find_packages, setup  # type: ignore

from version import __version__

setup(
    name="streamable",
    version=__version__,
    packages=find_packages(exclude=["tests"]),
    package_data={"streamable": ["py.typed"]},
    url="https://github.com/ebonnal/streamable",
    license="Apache 2.",
    author="ebonnal",
    author_email="bonnal.enzo.dev@gmail.com",
    description="concurrent & fluent interface for (async) iterables",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)
