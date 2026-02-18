"""Database connection and query utilities."""

import os
import pyodbc
import pandas as pd
from typing import Optional
from ..config import get_config


class DatabaseConnection:
    """Handle database connections for market data."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """Initialize database connection.
        
        Args:
            connection_string: Optional connection string. If None, uses config.
        """
        self.config = get_config()
        self.conn = None
        self.cursor = None
        
        if connection_string:
            self._connect_with_string(connection_string)
        elif self.config.database_enabled:
            self._connect_from_config()
    
    def _connect_from_config(self):
        """Connect to database using configuration."""
        server = self.config.get('database.server')
        database = self.config.get('database.database')
        username = self.config.get('database.username')
        password = self.config.get('database.password')
        driver = self.config.get('database.driver')
        
        if not all([server, database, username, password]):
            raise ValueError("Database configuration incomplete. Check config file or environment variables.")
        
        connection_string = (
            f'DRIVER={{{driver}}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        
        self._connect_with_string(connection_string)
    
    def _connect_with_string(self, connection_string: str):
        """Connect using a connection string."""
        try:
            self.conn = pyodbc.connect(connection_string)
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query string
            
        Returns:
            DataFrame with query results
        """
        if not self.conn:
            raise ConnectionError("Not connected to database")
        
        return pd.read_sql(sql, self.conn)
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
