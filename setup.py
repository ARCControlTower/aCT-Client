from setuptools import setup, find_packages

setup(
    name = 'aCT-client',
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description='Client tools to use with ARC Control Tower',
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url = 'http://github.com/ARCControlTower/aCT-Client',
    author = 'aCT team',
    author_email = 'act-dev@cern.ch',
    license="Apache 2.0",
    package_dir = {'': 'src'},
    packages=find_packages('src'),
    entry_points={
        'console_scripts': [
            'act = act_client.cli:main',
        ]
    },
    install_requires=[
        'cryptography',
        'pyyaml',
        'lark',
    ],
    python_requires='>=3.8',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
