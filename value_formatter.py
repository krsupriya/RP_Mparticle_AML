import datetime
import logging

def clean(value):
    value = value.strip()
    return value if value != "" else None

def email(value):
    if is_legit_email(value):
      return value.lower()
    else:
       logging.error(f'Wrong email syntax: {value}')
       raise ValueError(f'Wrong email syntax: {value}')

def boolean(value):
    return value.lower() == "true"

def datepol(value):
    try:
      datetime.date.fromisoformat(value)
    except ValueError:
      logging.error(f'Date is not ISO8601 format: {value}, returning None')
      return None
    return value

def name(value):
    return value.capitalize()

def phone(value):
    return value[:-2]

def is_legit_email(text):
    """ This method checks if the input email is legitimate.
    
    The method returns True if the input passes all criteria,
    and returns False if it violates one of the criteria
    mentioned below:
    
    - The input email cannot and must not be a NoneType instance.
    - The input email must contain only one "@" character.
    - The input text cannot have whitespaces.
    - The local section of the email can only be between 2 and 64 characters long.
    - The domain section of the email cannot exceed 255 characters.
    - The domain section must contain at least one '.' character.
    """

    # check if NoneType
    if text is None: return False

    # check at sign count
    if (text.count('@') != 1): return False
    # check for whitespaces
    if (text.count(' ') > 0): return False

    # split email to local and domain sections
    local, domain = text.split('@')

    # local section check
    if len(local) < 1 or len(local) > 64: return False

    # domain section check
    if (domain.count('.') < 1): return False
    if len(domain) > 255: return False

    return True