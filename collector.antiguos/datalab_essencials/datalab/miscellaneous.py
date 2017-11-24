import math, re

CSV_NUM_BYTESXLINE=12.0

def test_apply(x):
    """
    Filter FPAR function.
   
    :param x: datapoint to test
    :type x: iterable
    :return: True if FPAR else False
    :rtype: Boolean
    """
    val = str(x[9])
    try:
         float(val) #FPARS con keyvalue NaN y +-inf fallara a la hora de insertar y no se insertaran
         return True
    except (ValueError, TypeError):
        return False

def is_string(s):
    return isinstance(s, str)

def clean_for_kairos(s):
    """
    Simple kairos parser utility funcion for insert    

    :param s: a simple string value to insert in kairos  
    :type s: string 
    :return: value string parsed ready to insert in kairos  
    """
    removelist = r'[^A-Za-z0-9./_-]'
    return re.sub(removelist,"", s)

def is_keywvalue_num(keywvalue):
    try:
        float(keywvalue)
        return True
    except:
        return False

def es_get_type(data_point):
    if is_keywvalue_num(data_point[9]) and data_point[7].lower()=='fpar':
        return "opslog"
    else:
        return data_point[7].lower()

def parsing_keywvalue(keywvalue, mi_type):
    if mi_type == "fpar":
        return keywvalue
    elif mi_type=="opslog":
        return float(keywvalue)
    else:
        return keywvalue


