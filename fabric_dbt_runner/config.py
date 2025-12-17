"""
Configuration management for fabric-dbt-runner using Variable Libraries.
"""

from typing import Dict, Any, Optional
import os


class DbtConfig:
    """
    Manages dbt configuration using Fabric Variable Libraries.
    
    This class provides a bridge between Fabric's Variable Libraries
    and dbt's configuration requirements.
    """
    
    def __init__(
        self,
        project_dir: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        vars: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize dbt configuration.
        
        Args:
            project_dir: Path to dbt project directory
            profiles_dir: Path to dbt profiles directory
            target: Target environment (e.g., 'dev', 'prod')
            vars: Additional dbt variables
        """
        self.project_dir = project_dir or os.getcwd()
        self.profiles_dir = profiles_dir or os.path.expanduser("~/.dbt")
        self.target = target or "dev"
        self.vars = vars or {}
    
    def get_dbt_args(self) -> list:
        """
        Generate dbt command line arguments from configuration.
        
        Returns:
            List of command line arguments for dbt CLI
        """
        args = []
        
        if self.project_dir:
            args.extend(["--project-dir", self.project_dir])
        
        if self.profiles_dir:
            args.extend(["--profiles-dir", self.profiles_dir])
        
        if self.target:
            args.extend(["--target", self.target])
        
        if self.vars:
            import json
            args.extend(["--vars", json.dumps(self.vars)])
        
        return args
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return {
            "project_dir": self.project_dir,
            "profiles_dir": self.profiles_dir,
            "target": self.target,
            "vars": self.vars
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "DbtConfig":
        """
        Create configuration from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration values
            
        Returns:
            DbtConfig instance
        """
        return cls(
            project_dir=config_dict.get("project_dir"),
            profiles_dir=config_dict.get("profiles_dir"),
            target=config_dict.get("target"),
            vars=config_dict.get("vars")
        )
