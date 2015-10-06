from setuptools import setup, find_packages

__version__ = (0, 0, 1)

setup(
    name="crmp-helpers",
    description="A collection of scripts working with CRMP data",
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="Basil Veerman",
    author_email="bveerman@uvic.ca",
    packages=find_packages(),
    install_requires = ['pycds >= 2.0.0',
                        'sqlalchemy'],
    package_data = {
        'crmp-helpers': ["data/*.csv"],
        },
    include_package_data=True
    )
