from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()
    
setup(
    use_scm_version=True, 
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(include=['safe_backup', 'safe_backup.*']),
)
