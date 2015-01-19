from setuptools import setup, find_packages
setup(
    name = "metasync",
    version = "0.2",
    entry_points = {
        "console_scripts": [
            "metasync = metasync:main"
        ]},
    packages = find_packages(),
    package_data = {
            "metasync": ["google_client_secrets.json"]
    },
	install_requires = ['requests>=2.3.0', 'watchdog', 'pycrypto', 'selenium', 'urllib3', 'httplib2'],
)

