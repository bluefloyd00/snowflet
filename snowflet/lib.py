import os
import logging

def logging_config():
    logging.basicConfig(level=logging.INFO)

def default_account():
    """
    Returns ACCOUNT if env is set
    """
    return os.environ.get('ACCOUNT', '')

def default_user():
    """
    Returns USER if env is set
    """
    return os.environ.get('USER', '')

def default_password():
    """
    Returns PASSWORD if env is set
    """
    return os.environ.get('PASSWORD', '')

def default_database():
    """
    Returns DATABASE if env is set
    """
    return os.environ.get('DATABASE', '')

def default_schema():
    """
    Returns SCHEMA if env is set
    """
    return os.environ.get('SCHEMA', '')

def default_warehouse():
    """
    Returns WAREHOUSE if env is set
    """
    return os.environ.get('WAREHOUSE', '')

def default_role():
    """
    Returns ROLE if env is set
    """
    return os.environ.get('ROLE', '')

def default_timezone():
    """
    Returns TIMEZONE if env is set
    """
    return os.environ.get('TIMEZONE', ' europe/london')    
