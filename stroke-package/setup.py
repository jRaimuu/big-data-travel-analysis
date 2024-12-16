from setuptools import find_packages, setup

# Required dependencies
REQUIRED_PACKAGES = [
    'pandas',
    'requests',
    'beautifulsoup4',
    'tensorflow',
    'scikit-learn',
    'gdown==4.6.3',
    'google-cloud-storage',
    'python-json-logger'
]

setup(
    name='trainer',
    version='0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    description='My training application for Vertex AI.',
)
