from typing import *
import pyarrow as pa
import pathlib
import pyarrow.flight
import pyarrow.parquet


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """
    _name: str
    _input_types: List[pa.DataType]
    _result_type: pa.DataType

    def open(self):
        """
        Initialization method for the function. It is called before the actual working methods
        and thus suitable for one time setup work.
        """
        pass

    def close(self):
        """
        Tear-down method for the user code. It is called after the last call to the main
        working methods.
        """
        pass

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

    def eval_batch(self, batch: pa.RecordBatch) -> pa.Table:
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
        self._input_types = input_types
        self._result_type = result_type
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


def _create_udf(f, input_types, result_type, name):
    return UserDefinedFunctionWrapper(
        f, input_types, result_type, name)


def udf(f: Union[Callable, ScalarFunction, Type] = None,
        input_types: Union[List[pa.DataType], pa.DataType] = None,
        result_type: pa.DataType = None,
        name: str = None) -> Union[Callable, UserDefinedFunction]:
    """
    Helper method for creating a user-defined function.
    """
    # annotation
    if f is None:
        return lambda f: _create_udf(f, input_types, result_type, name)
    else:
        return _create_udf(f, input_types, result_type, name)


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
        """Return a FlightInfo."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        return pa.flight.FlightInfo(schema=udf.result_schema(), descriptor=descriptor, endpoints=[], total_records=0, total_bytes=0)

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf.full_name()
        print('added function:', name)
        self._functions[name] = udf

    def do_exchange(self, context, descriptor, reader, writer):
        """Run a simple echo server."""
        udf = self._functions[descriptor.path[0].decode('utf-8')]
        writer.begin(udf.result_schema())
        for chunk in reader:
            print(pa.Table.from_batches([chunk.data]))
            result = udf.eval_batch(chunk.data)
            writer.write_batch(result)

    def serve(self):
        """Start the server."""
        super(UdfServer, self).serve()
