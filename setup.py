from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()
    
setup(
    name='safe_backup',
    version='0.1.0',
    description="This program creates a secure backup of your files from a specified directory or an object storage location.",
    long_description=long_description,
    packages=find_packages(include=['safe_backup', 'safe_backup.*']),
    install_requires=[
        'async-timeout==4.0.3',
        'boto3==1.28.62',
        'botocore==1.31.62',
        'certifi==2023.7.22',
        'jmespath==1.0.1',
        'minio==7.1.17',
        'python-dateutil==2.8.2',
        'redis==5.0.0',
        's3transfer==0.7.0',
        'six==1.16.0',
        'urllib3==2.0.5',
    ],
    entry_points={
        'console_scripts': ['sbackup=safe_backup.safe_backup:main']
    },
)
