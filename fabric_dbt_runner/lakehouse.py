"""
Integration with Microsoft Fabric Lakehouse metadata.
"""

from typing import Dict, List, Optional, Any
import os


class LakehouseMetadata:
    """
    Manages integration with Fabric Lakehouse metadata.
    
    This class provides access to Lakehouse tables, schemas, and other
    metadata that can be used by dbt models.
    """
    
    def __init__(self, lakehouse_name: Optional[str] = None):
        """
        Initialize Lakehouse Metadata manager.
        
        Args:
            lakehouse_name: Name of the Fabric Lakehouse
        """
        self.lakehouse_name = lakehouse_name or os.getenv("FABRIC_LAKEHOUSE_NAME", "default")
        self.lakehouse_path = os.getenv("FABRIC_LAKEHOUSE_PATH", "/lakehouse/default")
    
    def get_lakehouse_path(self) -> str:
        """
        Get the path to the Lakehouse.
        
        Returns:
            Path to the Lakehouse
        """
        return self.lakehouse_path
    
    def get_tables_path(self) -> str:
        """
        Get the path to Lakehouse tables.
        
        Returns:
            Path to tables directory
        """
        return os.path.join(self.lakehouse_path, "Tables")
    
    def get_files_path(self) -> str:
        """
        Get the path to Lakehouse files.
        
        Returns:
            Path to files directory
        """
        return os.path.join(self.lakehouse_path, "Files")
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the Lakehouse.
        
        In a real Fabric environment, this would use the Fabric API.
        For development, it lists directories in the Tables path.
        
        Returns:
            List of table names
        """
        tables_path = self.get_tables_path()
        
        if not os.path.exists(tables_path):
            return []
        
        try:
            return [
                name for name in os.listdir(tables_path)
                if os.path.isdir(os.path.join(tables_path, name))
            ]
        except Exception:
            return []
    
    def get_table_location(self, table_name: str) -> str:
        """
        Get the storage location for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Path to the table location
        """
        return os.path.join(self.get_tables_path(), table_name)
    
    def get_lakehouse_config(self) -> Dict[str, Any]:
        """
        Get Lakehouse configuration for dbt profiles.
        
        Returns:
            Dictionary with Lakehouse configuration
        """
        return {
            "lakehouse_name": self.lakehouse_name,
            "lakehouse_path": self.lakehouse_path,
            "tables_path": self.get_tables_path(),
            "files_path": self.get_files_path()
        }
