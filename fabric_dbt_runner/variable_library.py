"""
Integration with Microsoft Fabric Variable Libraries.
"""

from typing import Dict, Any, Optional
import os


class VariableLibraryManager:
    """
    Manages integration with Fabric Variable Libraries.
    
    Variable Libraries in Fabric allow storing configuration values
    that can be accessed across notebooks and pipelines.
    """
    
    def __init__(self, library_name: Optional[str] = None):
        """
        Initialize Variable Library Manager.
        
        Args:
            library_name: Name of the Fabric Variable Library to use
        """
        self.library_name = library_name or "dbt_config"
        self._variables: Dict[str, Any] = {}
    
    def load_from_fabric(self) -> Dict[str, Any]:
        """
        Load variables from Fabric Variable Library.
        
        In a real Fabric environment, this would use the Fabric API
        to retrieve variables. For development/testing, it reads from
        environment variables prefixed with the library name.
        
        Returns:
            Dictionary of variables from the library
        """
        prefix = f"{self.library_name.upper()}_"
        variables = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                var_name = key[len(prefix):].lower()
                variables[var_name] = value
        
        self._variables = variables
        return variables
    
    def get_variable(self, key: str, default: Any = None) -> Any:
        """
        Get a variable from the library.
        
        Args:
            key: Variable key
            default: Default value if key not found
            
        Returns:
            Variable value or default
        """
        return self._variables.get(key, default)
    
    def set_variable(self, key: str, value: Any) -> None:
        """
        Set a variable in the library (local cache).
        
        Args:
            key: Variable key
            value: Variable value
        """
        self._variables[key] = value
    
    def get_all_variables(self) -> Dict[str, Any]:
        """
        Get all variables from the library.
        
        Returns:
            Dictionary of all variables
        """
        return self._variables.copy()
    
    def load_dbt_config(self) -> Dict[str, Any]:
        """
        Load dbt-specific configuration from Variable Library.
        
        Expected variables:
        - project_dir: Path to dbt project
        - profiles_dir: Path to dbt profiles
        - target: Target environment
        - vars: JSON string of additional variables
        
        Returns:
            Dictionary with dbt configuration
        """
        variables = self.load_from_fabric()
        
        config = {}
        if "project_dir" in variables:
            config["project_dir"] = variables["project_dir"]
        if "profiles_dir" in variables:
            config["profiles_dir"] = variables["profiles_dir"]
        if "target" in variables:
            config["target"] = variables["target"]
        if "vars" in variables:
            import json
            try:
                config["vars"] = json.loads(variables["vars"])
            except json.JSONDecodeError:
                config["vars"] = {}
        
        return config
