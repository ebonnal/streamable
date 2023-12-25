from setuptools import find_packages, setup # type: ignore

setup(
    name='streamable',
    version='0.0.4',
    packages=find_packages(),
    package_data={"streamable": ["py.typed"]},
    url='http://github.com/bonnal-enzo/streamable',
    license='Apache 2.',
    author='bonnal-enzo',
    author_email='bonnal.enzo.dev@gmail.com',
    description='Ease the manipulation of iterables.'
)
