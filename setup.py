from setuptools import setup, find_packages

setup(
    name="EMT_challenge",
    version=1.0,
    packages=find_packages(),
    description = "An exercise for my application to GP-CENIC",
    author="Carlos C Gutierrez S",
    author_email="carloscuauhtemoc@hotmail.com",
    license="Apache License 2.0",
    install_requires=['pandas', 'numpy', 'requests', 'bs4']
)

