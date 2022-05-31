from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="Swill",
    install_requires=[
        "Werkzeug >= 2.1",
        "msgspec == 0.6.0",
    ],
)
