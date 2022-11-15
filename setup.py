from setuptools import setup, find_packages
from typing import List

PROJECT_NAME = "finance complaint"
VERSION = "0.0.4"
AUTHOR = "WILSON CHARLES"
DESCRIPTION = "Financial data analysis"

REQUIREMENT_FILE_NAME = "requirements.txt"

HYPHEN_E_DOT = "-e ."

def get_requirements_list() -> List[str]:

    """
    description: Function returns the requirements to launch the project
    """

    with open(REQUIREMENT_FILE_NAME) as requirement_file:
        requirement_list = requirement_file.readlines()
        requirement_list = [requirement_name.replace("\n", "") for requirement_name in requirement_list]
        if HYPHEN_E_DOT in requirement_list:
            requirement_list.remove(HYPHEN_E_DOT)
        return requirement_list

setup(
    name = PROJECT_NAME,
    version = VERSION,
    author = AUTHOR,
    description = DESCRIPTION,
    find_packages = find_packages(),
    install_requires = get_requirements_list()
)