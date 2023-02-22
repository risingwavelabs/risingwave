from typing import *
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """
    _name: str
    _input_types: List[pa.DataType]
    _result_type: pa.DataType

    def full_name(self) -> str:
        """
        A unique name for the function. Composed by function name and input types.
        Example: "gcd/int32,int32"
        """
        return self._name + '/' + ','.join([str(t) for t in self._input_types])

    def result_schema(self) -> pa.Schema:
        """
        Returns the schema of the result table.
        """
        return pa.schema([('', self._result_type)])

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

    def eval(self, *args):
        """
        Method which defines the logic of the scalar function.
        """
        pass

    def eval_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        result = pa.array([self.eval(*[col[i].as_py() for col in batch])
                           for i in range(batch.num_rows)],
                          type=self._result_type)
        return pa.RecordBatch.from_arrays([result], schema=self.result_schema())


class UserDefinedFunctionWrapper(ScalarFunction):
    """
    Base Wrapper for Python user-defined function.
    """
    _func: Callable

    def __init__(self, func, input_types, result_type, name=None):
        self._func = func
        self._input_types = [_to_data_type(t) for t in input_types]
        self._result_type = _to_data_type(result_type)
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


def _create_udf(f, input_types, result_type, name):
    return UserDefinedFunctionWrapper(
        f, input_types, result_type, name)


def udf(input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
        result_type: Union[str, pa.DataType],
        name: Optional[str] = None,) -> Union[Callable, UserDefinedFunction]:
    """
    Annotation for creating a user-defined function.
    """

    return lambda f: _create_udf(f, input_types, result_type, name)


class UdfServer(pa.flight.FlightServerBase):
    """
    UDF server based on Apache Arrow Flight protocol.
    Reference: https://arrow.apache.org/cookbook/py/flight.html#simple-parquet-storage-service-with-arrow-flight
    """
    _functions: Dict[str, UserDefinedFunction]

    def __init__(self, location="grpc://0.0.0.0:8815", **kwargs):
        super(UdfServer, self).__init__(location, **kwargs)
        self._functions = {}

    def get_flight_info(self, context, descriptor):
        """Return the result schema of a function."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        return pa.flight.FlightInfo(schema=udf.result_schema(), descriptor=descriptor, endpoints=[], total_records=0, total_bytes=0)

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf.full_name()
        if name in self._functions:
            raise ValueError('Function already exists: ' + name)
        print('added function:', name)
        self._functions[name] = udf

    def do_exchange(self, context, descriptor, reader, writer):
        """Call a function from the client."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        writer.begin(udf.result_schema())
        for chunk in reader:
            # print(pa.Table.from_batches([chunk.data]))
            result = udf.eval_batch(chunk.data)
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
    match type_str:
        case 'BOOLEAN':
            return pa.bool_()
        case 'TINYINT':
            return pa.int8()
        case 'SMALLINT':
            return pa.int16()
        case 'INT' | 'INTEGER':
            return pa.int32()
        case 'BIGINT':
            return pa.int64()
        case 'FLOAT' | 'REAL':
            return pa.float32()
        case 'DOUBLE':
            return pa.float64()
        case 'DECIMAL':
            return pa.decimal128(38)
        case 'DATE':
            return pa.date32()
        case 'DATETIME':
            return pa.timestamp('ms')
        case 'TIME':
            return pa.time32('ms')
        case 'TIMESTAMP':
            return pa.timestamp('us')
        case 'CHAR' | 'VARCHAR':
            return pa.string()
        case 'BINARY' | 'VARBINARY':
            return pa.binary()
        case _:
            raise ValueError(f'Unsupported type: {type_str}')
