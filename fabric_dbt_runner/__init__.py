"""
fabric-dbt-runner: Native dbt runtime for Microsoft Fabric

This package allows you to execute dbt models natively inside Microsoft Fabric using:
- Fabric Notebooks
- Lakehouse metadata
- Variable Libraries

No external runners. No containers. No workarounds.
"""

__version__ = "0.1.0"

from .runner import DbtRunner
from .config import DbtConfig
from .variable_library import VariableLibraryManager

__all__ = ["DbtRunner", "DbtConfig", "VariableLibraryManager"]
