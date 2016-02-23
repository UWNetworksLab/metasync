from setuptools import setup, find_packages
import subprocess, os
from pkg_resources import parse_version
phantomjs_msg = "Metasync requires phantomjs>=1.9.2 binary in PATH"
try:
    v = subprocess.check_output(["phantomjs","--version"]).strip().decode("ascii")
    target_version = "1.9.2"
    if parse_version(v) < parse_version(target_version):
        raise Exception()
except:
    print(phantomjs_msg)
    exit(0)

setup(
    name = "metasync",
    version = "0.2.2",
    entry_points = {
        "console_scripts": [
            "metasync = metasync:main"
        ]},
    packages = find_packages(),
    package_data = {
            "metasync": ["google_client_secrets.json", "dropbox/trusted-certs.crt"]
    },
	install_requires = ['requests>=2.3.0', 'watchdog', 'pycrypto', 'selenium', 'urllib3', 'httplib2', 'google-api-python-client'],
)

