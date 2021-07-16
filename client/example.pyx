from pyarrow.lib cimport *

def get_array_length(obj):
    cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
    if arr.get() == NULL:
        raise TypeError("Not an array")
    return arr.get().length()