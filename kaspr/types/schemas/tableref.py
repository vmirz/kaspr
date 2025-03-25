"""Reference table definitions in agent and webview processing operators."""
import keyword
import re
from kaspr.types.schemas.base import BaseSchema
from kaspr.types.models import TableRefSpec
from marshmallow import fields

def valid_arg_name(name: str) -> bool:
    """
    Check if a string is a valid Python function argument name.
    
    Args:
        name (str): The string to validate.
    
    Returns:
        bool: True if the string is a valid argument name, False otherwise.
    """
    # Check if the string is empty
    if not name:
        return False
    
    # Check if the string is a Python keyword
    if keyword.iskeyword(name):
        return False
    
    # Check if the string matches the pattern for a valid Python identifier
    # Starts with a letter or underscore, followed by letters, digits, or underscores
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    return bool(re.match(pattern, name))

class TableRefSpecSchema(BaseSchema):
    __model__ = TableRefSpec

    name = fields.String(data_key="name", required=True)
    arg_name = fields.String(data_key="arg_name", required=True, validate=valid_arg_name)