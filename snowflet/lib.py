import os
import logging
import pandas 
import sql_metadata
from pandas._testing import assert_frame_equal
import unittest


def df_assert_equal(df1, df2):
    sort_dataframe(df1)
    sort_dataframe(df2)
    assert_frame_equal(df1, df2, check_like=True)

def sort_dataframe(df):
    df = df.apply(lambda x: x.sort_values().values)

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

def uppercase_parameters(func):
    def inner(*args, **kwargs):
        logging.info("Uppercase fields: database_id, schema_id, table_id")
        for key, value in kwargs.items():
            if key in ['database_id', 'schema_id', 'table_id']:
                kwargs[key] = value.upper()
        return func(*args, **kwargs)
    return inner


def forbiden_kwargs():
    return ['database_id', 'schema_id', 'table_id']


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

def uppercase_table(sql):
    query = sql
    for word in sql.split(" "):
        if is_table(word, query):
            query = query.replace(word, word.upper())
    return query

def add_table_prefix_to_sql(sql, prefix):
    query = sql
    for word in query.split(" "):
        if is_table(word, query) and prefix not in word:
            table = word
            db_prefixed =  prefix + "_" + word.split(".")[0].replace('"', '') 
            table = table.replace(word.split(".")[0], '"' + db_prefixed + '"' ).upper()           
            query = query.replace(word, table)
    return query


def is_table(word, sql):
    if word.replace('"','') in sql_metadata.get_query_tables(sql):
        return True
    else:
        return False

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

    if len(kwargs) > 0:
        sql = sql.format_map(SafeDict(**kwargs))

    if (kwargs.get('clone_database_prefix', None) is not None) and ("CLONE " not in sql):
        sql = add_table_prefix_to_sql(sql=sql, prefix=kwargs.get('clone_database_prefix', ''))
    else:
        sql = uppercase_table(sql=sql)
        
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


def add_database_id_prefix(obj, prefix, kwargs={}):

    if isinstance(obj, list):     
        for i in obj:
            add_database_id_prefix(i, prefix, kwargs)

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                for i in v:
                    add_database_id_prefix(i, prefix, kwargs)

            if isinstance(v, dict):
                apply_kwargs(v, kwargs)
                add_database_id_prefix(v, prefix, kwargs)
            else:
                if k == 'database_id': 
                    if str(prefix) not in obj[k]:
                        obj[k] =  str(prefix) + '_' + obj[k]


def extract_args(content, to_extract: str, kwargs={}):
    if len(kwargs) > 0:
        for x in content:
            apply_kwargs(x.get(to_extract), kwargs)
    return [x.get(to_extract, '') for x in content if x.get(to_extract, '') != '']


def apply_kwargs(orig, kwargs):
    if orig is not None:
        for key_, value_ in orig.items():
            for kwargs_key, kwargs_value in kwargs.items():
                if '$' + kwargs_key == value_:
                    orig.update({key_: kwargs_value})


def strip_table(table_name: str):
    if len(table_name.split(".")) == 3:
        return('"' + table_name.replace('"', '') + '"' )
    else:
        logging.error("Table definition not compliant with the framework, Database and schema shall be explicit")
        raise Exception


def extract_tables_from_query(sql_query: str):
    """ return a list of table_names """
    return [word for word in sql_query.split(" ") if len(word.split(".")) == 3]
