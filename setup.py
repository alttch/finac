__version__ = '0.5.8'

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='finac',
    version=__version__,
    author='Altertech',
    author_email='div@altertech.com',
    description='Financial accounting library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/alttch/finac',
    packages=setuptools.find_packages(),
    license='MIT',
    install_requires=[
        'rapidtables', 'python-dateutil', 'neotermcolor', 'sqlalchemy<2',
        'pyyaml', 'cachetools', 'flask', 'requests', 'pyaltt2>=0.0.89'
    ],
    classifiers=('Programming Language :: Python :: 3',
                 'License :: OSI Approved :: MIT License',
                 'Topic :: Software Development :: Libraries',
                 'Intended Audience :: Financial and Insurance Industry',
                 'Topic :: Office/Business :: Financial',
                 'Topic :: Office/Business :: Financial :: Accounting',
                 'Topic :: Office/Business :: Financial :: Investment'))
