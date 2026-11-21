"""Bindings for the native pipeline and the Arrow aggregation regression tool."""
import ctypes
import os
from pathlib import Path

_library = ctypes.CDLL(str(Path(__file__).resolve().with_name('aggregate.so')))
_library.aggregate_arrow.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
                                     ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]
_library.aggregate_arrow.restype = ctypes.c_int
_capsule_pointer = ctypes.pythonapi.PyCapsule_GetPointer
_capsule_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
_capsule_pointer.restype = ctypes.c_void_p


def aggregate_batches(reader, dictionary, campaigns, output):
    capsule = reader.__arrow_c_stream__()
    pointer = _capsule_pointer(capsule, b'arrow_array_stream')
    error = ctypes.create_string_buffer(4096)
    try:
        status = _library.aggregate_arrow(pointer, os.fsencode(dictionary), os.fsencode(campaigns),
                                          os.fsencode(output), error, len(error))
        if status:
            raise RuntimeError(error.value.decode('utf-8', errors='replace'))
    finally:
        reader.close()

_library.run_pipeline.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int64, ctypes.c_int64,
                                  ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
_library.run_pipeline.restype = ctypes.c_int


def run_pipeline(data, output, start, end, workers):
    error = ctypes.create_string_buffer(4096)
    status = _library.run_pipeline(os.fsencode(data), os.fsencode(output), start, end,
                                   workers, error, len(error))
    if status:
        raise RuntimeError(error.value.decode('utf-8', errors='replace'))
