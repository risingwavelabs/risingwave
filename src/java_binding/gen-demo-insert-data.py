import sys

def gen_row(index):
    v1 = int(index)
    v2 = int(index)
    v3 = int(index)
    v4 = float(index)
    v5 = float(index)
    v6 = index % 3 == 0
    v7 = str(index) * ((index % 10) + 1)
    may_null = None if index % 5 == 0 else int(index)
    row_data = [v1, v2, v3, v4, v5, v6, v7, may_null]
    repr = [o.__repr__() if o is not None else 'null' for o in row_data]
    return '(' + ', '.join(repr) + ')'


data_size = int(sys.argv[1])
data = [gen_row(i) for i in range(data_size)]
print(', '.join(data))
