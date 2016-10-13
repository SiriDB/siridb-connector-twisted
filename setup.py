from distutils.core import setup
import setuptools

VERSION = '2.0.0'

setup(
    name='siridb-connector-twisted',
    packages=[
        'siridb',
        'siridb.twisted',
        'siridb.twisted.lib'],
    version=VERSION,
    description='SiriDB Connector (using Twisted)',
    author='Jeroen van der Heijden',
    author_email='jeroen@transceptor.technology',
    url='https://github.com/transceptor-technology/siridb-connector-twisted',
    download_url=
        'https://github.com/transceptor-technology/'
        'siridb-connector-twisted/tarball/{}'.format(VERSION),
    keywords=['siridb', 'connector', 'database', 'client', 'twisted'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development'
    ],
    install_requires=['qpack']
)
