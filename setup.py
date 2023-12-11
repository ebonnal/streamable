from setuptools import find_packages, setup # type: ignore

setup(
    name='kioss',
    version='0.8.0',
    packages=find_packages(),
    package_data={"kioss": ["py.typed"]},
    url='http://github.com/bonnal-enzo/kioss',
    license='Apache 2.',
    author='bonnal-enzo',
    author_email='bonnal.enzo.dev@gmail.com',
    description='Keep I/O Simple and Stupid: Library providing a expressive Iterator-based interface to write ETL pipelines.'
)
