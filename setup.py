from setuptools import setup, find_packages

setup(
    name='ibk',
    version='0.1.1',
    author='Christopher Miller',
    author_email='cmm801@gmail.com',
    packages=find_packages(),
    scripts=[],
    url='http://pypi.python.org/pypi/ibk/',
    license='MIT',
    description='A package for working with Interactive Brokers TWS API.',
    long_description=open('README.md').read(),
    install_requires=[
        'numpy', 'pandas', 'ibapi'
        ],
)
