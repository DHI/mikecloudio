import setuptools

setuptools.setup(
    name="mikecloudio",
    version='0.0.2',
    install_requires=["pandas", "shapely"],
    url="https://github.com/rhaDHI/mikecloudio",
    packages=setuptools.find_packages(),
    include_package_data=True,
)
