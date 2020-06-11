from setuptools import setup
import setuptools
setup(
    name="pipee",
    version='1.0.0',
    url='https://github.com/wylswz/pipee',
    author='Yunlu Wen',
    author_email='looooooeee@gmail.com',
    description='Pipeline & track your tasks',
    license="Apache License, Version 2.0",
    packages=setuptools.find_packages(),
    keywords=['utility', 'pipeline'],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
   ],
    install_requires=[
        "SQLAlchemy",
        "mysqlclient==1.4.6",
        "pydantic==1.5.1",
        "python-dotenv==0.13.0"
    ],
)