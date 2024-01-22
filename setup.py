from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()
    
setup(    
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/vahidrnaderi/safe_backup',
    packages=find_packages(include=['safe_backup', 'safe_backup.*']),
)
