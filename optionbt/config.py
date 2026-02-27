"""Configuration management for option backtest framework."""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Configuration manager for the backtest framework."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration.
        
        Args:
            config_file: Path to YAML config file. If None, uses defaults.
        """
        self.config_file = config_file
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or use defaults."""
        default_config = {
            'database': {
                'enabled': False,
                'server': os.getenv('DB_SERVER', ''),
                'database': os.getenv('DB_NAME', ''),
                'username': os.getenv('DB_USERNAME', ''),
                'password': os.getenv('DB_PASSWORD', ''),
                'driver': 'ODBC Driver 17 for SQL Server'
            },
            'paths': {
                'data_dir': 'data_download',
                'output_dir': 'output',
                'config_dir': 'runs'
            },
            'backtest': {
                'base_currency': 'USD',
                'reinvest_premium': True,
                'eod_pricing_method': 'mid'
            }
        }
        
        if self.config_file and os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    file_config = yaml.safe_load(f) or {}
                    # Merge with defaults
                    return self._merge_dicts(default_config, file_config)
            except Exception:
                # If YAML not available or file invalid, use defaults
                pass
        
        return default_config
    
    def _merge_dicts(self, base: Dict, override: Dict) -> Dict:
        """Recursively merge dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key using dot notation.
        
        Args:
            key: Configuration key (e.g., 'database.server')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    @property
    def database_enabled(self) -> bool:
        """Check if database is enabled."""
        return self.get('database.enabled', False)
    
    @property
    def data_dir(self) -> Path:
        """Get data directory path."""
        return Path(self.get('paths.data_dir', 'data_download'))
    
    @property
    def output_dir(self) -> Path:
        """Get output directory path."""
        return Path(self.get('paths.output_dir', 'output'))
    
    @property
    def config_dir(self) -> Path:
        """Get config directory path."""
        return Path(self.get('paths.config_dir', 'runs'))


# Global config instance
_config = None


def get_config(config_file: Optional[str] = None) -> Config:
    """Get or create global configuration instance.
    
    Args:
        config_file: Path to config file
        
    Returns:
        Config instance
    """
    global _config
    if _config is None or config_file is not None:
        _config = Config(config_file)
    return _config
