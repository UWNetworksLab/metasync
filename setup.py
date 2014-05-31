from setuptools import setup, find_packages
setup(
    name = "metasync",
    version = "0.1",
    entry_points = {
        "console_scripts": [
            "metasync = metasync:main"
        ]},
    packages = find_packages(),
    package_data = {
            "metasync": ["google_client_secrets.json"]
    }
)

