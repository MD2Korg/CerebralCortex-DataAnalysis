import os
from os import path
from pip.req import parse_requirements
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

install_reqs = parse_requirements("requirements.txt", session='hack')
reqs = [str(ir.req) for ir in install_reqs]

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()

datadir = os.path.join('core','resources')
datafiles = [(d, [os.path.join(d,f) for f in files])
                 for d, folders, files in os.walk(datadir)]
print('AAAAAAAAAA',datafiles)

setup(
    name="MD2K: Cerebral Cortex: DataAnalysis compute features",

    version='2.2.1',

    description='',
    long_description=long_description,

    author='MD2K.org',
    author_email='software@md2k.org',

    license='BSD2',

    classifiers=[

        'Development Status :: 1 - Beta',

        'Intended Audience :: Healthcare Industry',
        'Intended Audience :: Science/Research',

        'License :: OSI Approved :: BSD License',

        'Natural Language :: English',

        'Programming Language :: Python :: 3',

        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Distributed Computing'
    ],

    keywords='mHealth machine-learning data-analysis',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    #install_requires=reqs,

    entry_points={},

    #data_files = [('', ['core/resources/models/posture_randomforest.model'])]
    data_files = datafiles
)
