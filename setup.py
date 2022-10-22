#!/usr/bin/env python

from distutils.core import setup

setup(
    name='reddit_sentiment',
    version='1.0',
    description='A reddit sentiment analysis toolkit',
    author='Lennard Berger',
    url='https://github.com/Fohlen/reddit-sentiment',
    packages=['reddit_sentiment'],
    package_data={
        'reddit_sentiment': ['preprocess_archive.sh']
    },
    scripts=[
        'bin/distill-dataset',
        'bin/download-annotate-archive',
    ],
    entry_points={
        'console_scripts': ['download-annotate-archives=reddit_sentiment.command_line:main'],
    },
    install_requires=[
        "requests>=2.28.1",
        "textblob>=0.17.1",
        "tqdm>=4.64.1",
        "pyspark>=3.3.0"
    ]
)
