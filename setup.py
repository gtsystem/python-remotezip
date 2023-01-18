from setuptools import setup

with open("README.md") as f:
    description = f.read()

setup(
    name='remotezip',
    version='0.12.1',
    author='Giuseppe Tribulato',
    author_email='gtsystem@gmail.com',
    py_modules=['remotezip'],
    url='https://github.com/gtsystem/python-remotezip',
    license='MIT',
    description='Access zip file content hosted remotely without downloading the full file.',
    long_description=description,
    long_description_content_type="text/markdown",
    install_requires=["requests", "tabulate"],
    tests_require=['requests_mock'],
    scripts=['bin/remotezip'],
    test_suite='test_remotezip',
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ]
)
