"""
Main dbt runner for Microsoft Fabric.
"""

from typing import Dict, List, Optional, Any
import subprocess
import sys
import os
from pathlib import Path

from .config import DbtConfig
from .variable_library import VariableLibraryManager
from .lakehouse import LakehouseMetadata


class DbtRunner:
    """
    Main class for running dbt commands in Microsoft Fabric.
    
    This runner integrates with:
    - Fabric Notebooks for execution
    - Variable Libraries for configuration
    - Lakehouse metadata for data access
    """
    
    def __init__(
        self,
        config: Optional[DbtConfig] = None,
        variable_library: Optional[str] = None,
        lakehouse_name: Optional[str] = None
    ):
        """
        Initialize the dbt runner.
        
        Args:
            config: DbtConfig instance or None to auto-configure
            variable_library: Name of Variable Library to use
            lakehouse_name: Name of Lakehouse to use
        """
        self.variable_manager = VariableLibraryManager(variable_library)
        self.lakehouse = LakehouseMetadata(lakehouse_name)
        
        # Load configuration from Variable Library if not provided
        if config is None:
            var_config = self.variable_manager.load_dbt_config()
            if var_config:
                config = DbtConfig.from_dict(var_config)
            else:
                config = DbtConfig()
        
        self.config = config
        self._last_result: Optional[subprocess.CompletedProcess] = None
    
    def run(
        self,
        models: Optional[List[str]] = None,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: bool = False,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Run dbt models.
        
        Args:
            models: List of specific models to run
            select: dbt selection syntax
            exclude: dbt exclusion syntax
            full_refresh: Force full refresh of incremental models
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "run"]
        cmd.extend(self.config.get_dbt_args())
        
        if models:
            for model in models:
                cmd.extend(["--models", model])
        
        if select:
            cmd.extend(["--select", select])
        
        if exclude:
            cmd.extend(["--exclude", exclude])
        
        if full_refresh:
            cmd.append("--full-refresh")
        
        # Add any additional keyword arguments
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def test(
        self,
        models: Optional[List[str]] = None,
        select: Optional[str] = None,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Run dbt tests.
        
        Args:
            models: List of specific models to test
            select: dbt selection syntax
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "test"]
        cmd.extend(self.config.get_dbt_args())
        
        if models:
            for model in models:
                cmd.extend(["--models", model])
        
        if select:
            cmd.extend(["--select", select])
        
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def build(
        self,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: bool = False,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Run dbt build (run + test in dependency order).
        
        Args:
            select: dbt selection syntax
            exclude: dbt exclusion syntax
            full_refresh: Force full refresh of incremental models
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "build"]
        cmd.extend(self.config.get_dbt_args())
        
        if select:
            cmd.extend(["--select", select])
        
        if exclude:
            cmd.extend(["--exclude", exclude])
        
        if full_refresh:
            cmd.append("--full-refresh")
        
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def compile(
        self,
        models: Optional[List[str]] = None,
        select: Optional[str] = None,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Compile dbt models without running them.
        
        Args:
            models: List of specific models to compile
            select: dbt selection syntax
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "compile"]
        cmd.extend(self.config.get_dbt_args())
        
        if models:
            for model in models:
                cmd.extend(["--models", model])
        
        if select:
            cmd.extend(["--select", select])
        
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def debug(self) -> subprocess.CompletedProcess:
        """
        Run dbt debug to check configuration.
        
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "debug"]
        cmd.extend(self.config.get_dbt_args())
        return self._execute_command(cmd)
    
    def deps(self) -> subprocess.CompletedProcess:
        """
        Install dbt dependencies.
        
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "deps"]
        cmd.extend(self.config.get_dbt_args())
        return self._execute_command(cmd)
    
    def seed(
        self,
        select: Optional[str] = None,
        full_refresh: bool = False,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Load seed files into the data warehouse.
        
        Args:
            select: dbt selection syntax
            full_refresh: Force full refresh of seeds
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "seed"]
        cmd.extend(self.config.get_dbt_args())
        
        if select:
            cmd.extend(["--select", select])
        
        if full_refresh:
            cmd.append("--full-refresh")
        
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def snapshot(self, select: Optional[str] = None, **kwargs) -> subprocess.CompletedProcess:
        """
        Execute dbt snapshots.
        
        Args:
            select: dbt selection syntax
            **kwargs: Additional dbt arguments
            
        Returns:
            CompletedProcess with command results
        """
        cmd = ["dbt", "snapshot"]
        cmd.extend(self.config.get_dbt_args())
        
        if select:
            cmd.extend(["--select", select])
        
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif not isinstance(value, bool):
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
        
        return self._execute_command(cmd)
    
    def _execute_command(self, cmd: List[str]) -> subprocess.CompletedProcess:
        """
        Execute a dbt command.
        
        Args:
            cmd: Command and arguments to execute
            
        Returns:
            CompletedProcess with command results
        """
        print(f"Executing: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=self.config.project_dir
        )
        
        # Print output for visibility in notebooks
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        self._last_result = result
        
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt command failed with exit code {result.returncode}\n"
                f"Command: {' '.join(cmd)}\n"
                f"Error: {result.stderr}"
            )
        
        return result
    
    def get_last_result(self) -> Optional[subprocess.CompletedProcess]:
        """
        Get the result of the last executed command.
        
        Returns:
            CompletedProcess or None if no commands executed
        """
        return self._last_result
    
    def get_lakehouse_info(self) -> Dict[str, Any]:
        """
        Get information about the connected Lakehouse.
        
        Returns:
            Dictionary with Lakehouse information
        """
        return {
            **self.lakehouse.get_lakehouse_config(),
            "tables": self.lakehouse.list_tables()
        }
