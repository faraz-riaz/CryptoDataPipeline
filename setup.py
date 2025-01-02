# setup.py
from setuptools import setup, find_packages

setup(
    name="crypto-pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'requests==2.31.0',
        'python-dotenv==1.0.0',
        'pandas==2.2.3',
        'pytest==7.4.0',
        'black==23.7.0',
        'pylint==2.17.5',
        'logging==0.4.9.6',
        'pyarrow==18.1.0',
        'apache-airflow==2.10.4',
        'kafka==1.3.5',
        'kafka-python==2.0.2',
        'pyspark==3.5.4',
        'google_cloud_bigquery-3.27.0',
        'pandas_gbq-0.26.0'
    ]
)