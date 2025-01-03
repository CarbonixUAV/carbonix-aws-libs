from setuptools import setup, find_packages

setup(
    name="carbonix-aws-libs",
    version="1.0",
    description="Python libraries for AWS tools supporting the Carbonix log analysis system.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Lokesh",
    url="https://github.com/CarbonixUAV/carbonix-aws-libs",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.28.0",
        "pymysql>=1.0.0",
        "python-dotenv>=1.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
)
