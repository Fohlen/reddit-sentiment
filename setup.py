#!/usr/bin/env python

from distutils.core import setup

setup(
    name='reddit_sentiment',
    version='1.0',
    description='A reddit sentiment analysis toolkit',
    author='Lennard Berger',
    url='https://github.com/Fohlen/reddit-sentiment',
    include_package_data=True,
    packages=['reddit_sentiment'],
    scripts=[
        'bin/distill-dataset',
        'bin/download-annotate-archive',
        'bin/download-annotate-archives'
    ],
    install_requires=[
        "requests>=2.28.1",
        "textblob>=0.17.1",
        "tqdm>=4.64.1",
        "pyspark>=3.3.0"
    ]
)
