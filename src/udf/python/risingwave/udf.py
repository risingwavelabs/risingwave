from typing import *
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import inspect
import traceback


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """
    _name: str
    _input_schema: pa.Schema
    _result_schema: pa.Schema

    def eval_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Apply the function on a batch of inputs.
        """
        pass


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.
    """

    def eval(self, *args) -> Any:
        """
        Method which defines the logic of the scalar function.
        """
        pass

    def eval_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        column = [self.eval(*[col[i].as_py() for col in batch])
                  for i in range(batch.num_rows)]
        array = pa.array(column, type=self._result_schema.types[0])
        return pa.RecordBatch.from_arrays([array], schema=self._result_schema)


class TableFunction(UserDefinedFunction):
    """
    Base interface for user-defined table function. A user-defined table functions maps zero, one,
    or multiple table values to a new table value.
    """

    def eval(self, *args) -> Iterator:
        """
        Method which defines the logic of the table function.
        """
        yield

    def eval_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        result_rows = []
        # Iterate through rows in the input RecordBatch
        for row_index in range(batch.num_rows):
            row = tuple(column[row_index].as_py() for column in batch)
            result_rows.extend(self.eval(*row))

        result_columns = zip(
            *result_rows) if len(self._result_schema) > 1 else [result_rows]

        # Convert the result columns to arrow arrays
        arrays = [
            pa.array(col, type) for col, type in zip(result_columns, self._result_schema.types)
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=self._result_schema)


class UserDefinedScalarFunctionWrapper(ScalarFunction):
    """
    Base Wrapper for Python user-defined scalar function.
    """
    _func: Callable

    def __init__(self, func, input_types, result_type, name=None):
        self._func = func
        self._input_schema = pa.schema(zip(
            inspect.getfullargspec(func)[0],
            [_to_data_type(t) for t in _to_list(input_types)]
        ))
        self._result_schema = pa.schema(
            [('output', _to_data_type(result_type))])
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


class UserDefinedTableFunctionWrapper(TableFunction):
    """
    Base Wrapper for Python user-defined table function.
    """
    _func: Callable

    def __init__(self, func, input_types, result_types, name=None):
        self._func = func
        self._input_schema = pa.schema(zip(
            inspect.getfullargspec(func)[0],
            [_to_data_type(t) for t in _to_list(input_types)]
        ))
        self._result_schema = pa.schema(
            [('', _to_data_type(t)) for t in _to_list(result_types)])
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


def _to_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


def udf(input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
        result_type: Union[str, pa.DataType],
        name: Optional[str] = None,) -> Union[Callable, UserDefinedFunction]:
    """
    Annotation for creating a user-defined scalar function.

    Parameters:
    - input_types: A list of strings or Arrow data types that specifies the input data types.
    - result_type: A string or an Arrow data type that specifies the return value type.
    - name: An optional string specifying the function name. If not provided, the original name will be used.

    Example:
    ```
    @udf(input_types=['INT', 'INT'], result_type='INT')
    def gcd(x, y):
        while y != 0:
            (x, y) = (y, x % y)
        return x
    ```
    """

    return lambda f: UserDefinedScalarFunctionWrapper(f, input_types, result_type, name)


def udtf(input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
         result_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
         name: Optional[str] = None,) -> Union[Callable, UserDefinedFunction]:
    """
    Annotation for creating a user-defined table function.

    Parameters:
    - input_types: A list of strings or Arrow data types that specifies the input data types.
    - result_types A list of strings or Arrow data types that specifies the return value types.
    - name: An optional string specifying the function name. If not provided, the original name will be used.

    Example:
    ```
    @udtf(input_types='INT', result_types='INT')
    def series(n):
        for i in range(n):
            yield i
    ```
    """

    return lambda f: UserDefinedTableFunctionWrapper(f, input_types, result_types, name)


class UdfServer(pa.flight.FlightServerBase):
    """
    A server that provides user-defined functions to clients.

    Example:
    ```
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(my_udf)
    server.serve()
    ```
    """
    # UDF server based on Apache Arrow Flight protocol.
    # Reference: https://arrow.apache.org/cookbook/py/flight.html#simple-parquet-storage-service-with-arrow-flight

    _functions: Dict[str, UserDefinedFunction]

    def __init__(self, location="0.0.0.0:8815", **kwargs):
        super(UdfServer, self).__init__('grpc://' + location, **kwargs)
        self._functions = {}

    def get_flight_info(self, context, descriptor):
        """Return the result schema of a function."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        # return the concatenation of input and output schema
        full_schema = pa.schema(
            list(udf._input_schema) + list(udf._result_schema))
        # we use `total_records` to indicate the number of input arguments
        return pa.flight.FlightInfo(schema=full_schema, descriptor=descriptor, endpoints=[], total_records=len(udf._input_schema), total_bytes=0)

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf._name
        if name in self._functions:
            raise ValueError('Function already exists: ' + name)
        print('added function:', name)
        self._functions[name] = udf

    def do_exchange(self, context, descriptor, reader, writer):
        """Call a function from the client."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        writer.begin(udf._result_schema)
        for chunk in reader:
            # print(pa.Table.from_batches([chunk.data]))
            try:
                result = udf.eval_batch(chunk.data)
            except Exception as e:
                print(traceback.print_exc())
                raise e
            writer.write_batch(result)

    def serve(self):
        """Start the server."""
        super(UdfServer, self).serve()


def _to_data_type(t: Union[str, pa.DataType]) -> pa.DataType:
    """
    Convert a string or pyarrow.DataType to pyarrow.DataType.
    """
    if isinstance(t, str):
        return _string_to_data_type(t)
    else:
        return t


def _string_to_data_type(type_str: str):
    type_str = type_str.upper()
    if type_str in ('BOOLEAN', 'BOOL'):
        return pa.bool_()
    elif type_str in ('SMALLINT', 'INT2'):
        return pa.int16()
    elif type_str in ('INT', 'INTEGER', 'INT4'):
        return pa.int32()
    elif type_str in ('BIGINT', 'INT8'):
        return pa.int64()
    elif type_str in ('FLOAT4', 'REAL'):
        return pa.float32()
    elif type_str in ('FLOAT8', 'DOUBLE PRECISION'):
        return pa.float64()
    elif type_str.startswith('DECIMAL') or type_str.startswith('NUMERIC'):
        return pa.decimal128(38)
    elif type_str in ('DATE'):
        return pa.date32()
    elif type_str in ('TIME', 'TIME WITHOUT TIME ZONE'):
        return pa.time32('ms')
    elif type_str in ('TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE'):
        return pa.timestamp('ms')
    elif type_str.startswith('INTERVAL'):
        return pa.duration('us')
    elif type_str in ('VARCHAR'):
        return pa.string()
    elif type_str in ('BYTEA'):
        return pa.binary()
    elif type_str.startswith('STRUCT'):
        # extract 'STRUCT<a INT, b VARCHAR, ...>'
        type_str = type_str[6:].strip('<>')
        fields = []
        for field in type_str.split(','):
            field = field.strip()
            name, type_str = field.split(' ')
            fields.append(pa.field(name, _string_to_data_type(type_str)))
        return pa.struct(fields)

    raise ValueError(f'Unsupported type: {type_str}')
