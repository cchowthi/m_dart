import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="package_name_here",
    version="0.0.1",
    author="your_name_here",
    author_email="your_email_here",
    description="a_short_simple_description_of_your_package_here",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gilead.com",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    include_package_data=True,
    zip_safe=False
)
