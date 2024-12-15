from setuptools import find_packages
from setuptools import setup

# Specify the required dependencies
REQUIRED_PACKAGES = [
    'pandas',
    'requests',
    'beautifulsoup4',
    'pygam',
    'tensorflow',
    'scikit-learn',
    'gdown==4.6.3'
]

setup(
    name='trainer',  # Name of your package
    version='0.1',  # Version of your package
    install_requires=REQUIRED_PACKAGES,  # Dependencies required
    packages=find_packages(),  # Automatically find and include all packages
    include_package_data=True,  # Include non-code files specified in MANIFEST.in
    description='My training application for Vertex AI.',  # Short description
)