
def bytes_xor(a, b):
    return bytes(_a ^ _b for (_a, _b) in zip(a, b))

def bytes_to_int(a):
    return int.from_bytes(a, 'little', signed=True)

def bytes_to_uint(a):
    return int.from_bytes(a, 'little', signed=False)

def int_to_bytes(a):
    return a.to_bytes(4, 'little', signed=True)

def uint_to_bytes(a):
    return a.to_bytes(4, 'little', signed=False)
