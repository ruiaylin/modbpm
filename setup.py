from setuptools import setup, find_packages

setup(
    name="modbpm",
    version="0.1.1a1",
    author="sveinchen",
    author_email="sveinchen@gmail.com",
    url="https://modbpm.github.io",
    packages=find_packages(include=['modbpm',
                                    'modbpm.*']),
    install_requires=[
        'Django>=1.8.3,<1.9',
        'Celery<4.0',
        'django-celery',
    ],
    include_package_data=True,
    zip_safe=False,
)
