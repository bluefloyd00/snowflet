import os
import logging

def print_kwargs_params(func):
    def inner(*args, **kwargs):
        logging.info("Keyword args applied to the template:")
        for key, value in kwargs.items():
            if key in forbiden_kwargs():
                raise KeyError("{} is a forbidden keyword argument.".format(key))
        for key, value in kwargs.items():
            logging.info("%s = %s" % (key, value))
        return func(*args, **kwargs)
    return inner

def forbiden_kwargs():
    return ['list_of_dedicated_keywords']


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

@print_kwargs_params
def read_sql(file='', query="", *args, **kwargs):
    ''' Read SQL file and apply arguments/keywors arguments.

    Args:
        file (string): path to the SQL file from PROJECT_ROOT environment variable.
        *kwargs can be passed if some parameters are to be passed.

    Returns:
        a SQL formated with **kwargs if applicable.

    Example:
        With SQL as:
            "select .. {param2} .. {param1} .. {paramN}"
        *kwargs as:
            param1=value1
            param2=value2
            paranN=valueN
        The functions returns:
            "select .. value2 .. value1 .. valueN"
    '''
    if file == '' and query == '':
        logging.info("Either File or query parameter shall be passed to the function")
        raise Exception
    
    if query != '':
        sql = query
    else:
        path_to_file = os.path.join(os.getenv("PROJECT_ROOT"), file)
        file = open(path_to_file, 'r')
        sql = file.read()
        file.close()

    if kwargs.get('dry_run_dataset_prefix', None) is not None:
        for index, dataset in enumerate(sql.split("`")):
            if index%2==1 and "." in dataset:
                sql = sql.replace(
                    "`" + sql.split("`")[index] + "`",
                    "`" + str(kwargs.get('dry_run_dataset_prefix', None)) + "_" +  sql.split("`")[index] + "`",
                    1
                )

    if len(kwargs) > 0:
        sql = sql.format_map(SafeDict(**kwargs))
    return sql

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
