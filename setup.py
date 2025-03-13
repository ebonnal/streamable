from setuptools import find_packages, setup  # type: ignore
from version import __version__

setup(
    name="streamable",
    version=__version__,
    packages=find_packages(exclude=["tests"]),
    package_data={"streamable": ["py.typed"]},
    url="http://github.com/ebonnal/streamable",
    license="Apache 2.",
    author="ebonnal",
    author_email="bonnal.enzo.dev@gmail.com",
    description="Pythonic Stream-like manipulation of iterables",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)
