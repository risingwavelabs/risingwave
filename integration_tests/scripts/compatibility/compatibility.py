#!/usr/bin/python3

import yaml


class DataType:
    def __init__(self, name: str, zero=None, minimum=None, maximum=None, maximum_gen_py="", null="null", aliases=None,
                 rw_type=None):
        self.name = name
        self.col_name = "c_" + self.name.replace(" ", "_")
        self.array_col_name = self.col_name + "_array"
        self.aliases = aliases
        self.zero = zero
        self.min = minimum
        self.max = maximum
        self.null = null
        self.rw_type = rw_type
        if maximum_gen_py != "":
            exec("self.max={}".format(maximum_gen_py))

    def cast(self, value):
        return '{}::{}'.format(value, self.name)

    def array_cast(self, value):
        return '{}::{}'.format(value, self.array_type())

    def array_type(self):
        return self.name + "[]"

    def array_zero(self):
        return "array[]"

    def array_min(self):
        return "array[{}]".format(self.cast(self.min))

    def array_max(self):
        return "array[{}]".format(self.cast(self.max))

    def select_zero_sql(self):
        return "SELECT {};".format(self.cast(self.zero))

    def select_min_sql(self):
        return "SELECT {};".format(self.cast(self.min))

    def select_max_sql(self):
        return "SELECT {};".format(self.cast(self.max))

    def select_array_zero_sql(self):
        return "SELECT {};".format(self.array_cast(self.array_zero()))

    def select_array_min_sql(self):
        return "SELECT {}".format(self.array_cast(self.array_min()))

    def select_array_max_sql(self):
        return "SELECT {}".format(self.array_cast(self.array_max()))


class MysqlDataType(DataType):
    def cast(self, value):
        return "CAST({} AS {})".format(value, self.name)


class TableSqlGenerator:
    def __init__(self, name: str, enable_array: bool, enable_struct: bool, pk_types: list[str],
                 datatypes: list[DataType]):
        self.table_name = name
        self.pk_types = pk_types
        self.datatypes = datatypes
        self.enable_array = enable_array
        self.enable_struct = enable_struct
        self.null = "null"

    def struct_type(self):
        pass

    def struct_values(self, value):
        return 'ROW({})'.format(value)

    def create_table_sql(self):
        prefix = "CREATE TABLE IF NOT EXISTS " + self.table_name
        cols = "(\n"
        pk_col_names = []
        for data_type in self.datatypes:
            if cols != "(\n":
                cols += ",\n"
            cols += data_type.col_name + " " + data_type.name
            if data_type.name in self.pk_types:
                pk_col_names.append(data_type.col_name)
        if self.enable_array:
            for data_type in self.datatypes:
                cols += ",\n"
                cols += data_type.array_col_name + " " + data_type.array_type()
        if self.enable_struct:
            cols += ",\n"
            cols += "c_struct {}".format(self.struct_type())
        if self.pk_types:
            cols += ",\nPRIMARY KEY (" + ",".join(pk_col_names) + ")\n);"
        return prefix + cols

    def drop_table_sql(self):
        return "DROP TABLE IF EXISTS {};".format(self.table_name)

    def insert_null_sql(self, ):
        prefix = "INSERT INTO " + self.table_name + "VALUES ("
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ","
            cols += data_type.null
        if self.enable_array:
            for data_type in self.datatypes:
                cols += ","
                cols += data_type.null
        if self.enable_struct:
            cols += "," + self.null
        return prefix + cols + ");"

    def zero_values(self):
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ","
            cols = '{} {}'.format(cols, data_type.zero)
        if self.enable_array:
            for data_type in self.datatypes:
                cols = '{}, {}'.format(cols, data_type.array_cast(data_type.array_zero()))
        if self.enable_struct:
            cols += "," + self.struct_values(cols)
        return cols

    def min_values(self):
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ","
            cols = '{} {}'.format(cols, data_type.min)
        if self.enable_array:
            for data_type in self.datatypes:
                cols = '{}, {}'.format(cols, data_type.array_cast(data_type.array_min()))
        if self.enable_struct:
            cols += "," + self.struct_values(cols)
        return cols

    def max_values(self):
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ","
            cols = '{} {}'.format(cols, data_type.max)
        if self.enable_array:
            for data_type in self.datatypes:
                cols = '{}, {}'.format(cols, data_type.array_cast(data_type.array_max()))
        if self.enable_struct:
            cols += "," + self.struct_values(cols)
        return cols

    def insert_zero_sql(self):
        prefix = "INSERT INTO " + self.table_name + " VALUES ("
        cols = self.zero_values()
        return prefix + cols + ");"

    def insert_min_sql(self):
        prefix = "INSERT INTO " + self.table_name + " VALUES ("
        cols = self.min_values()
        return prefix + cols + ");"

    def insert_max_sql(self):
        prefix = "INSERT INTO " + self.table_name + " VALUES ("
        cols = self.max_values()
        return prefix + cols + ");"


class RisingwaveTableSqlGenerator(TableSqlGenerator):
    def struct_type(self):
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ",\n"
            cols = cols + data_type.col_name + " " + data_type.name
        if self.enable_array:
            for data_type in self.datatypes:
                cols += ",\n"
                cols = cols + data_type.array_col_name + " " + data_type.array_type()
        return "struct <\n{}\n>".format(cols)


class PostgresTableSqlGenerator(TableSqlGenerator):
    def struct_type(self):
        cols = ""
        for data_type in self.datatypes:
            if cols != "":
                cols += ",\n"
            cols = cols + data_type.col_name + " " + data_type.name
        if self.enable_array:
            for data_type in self.datatypes:
                cols += ",\n"
                cols = cols + data_type.array_col_name + " " + data_type.array_type()
        print("DROP TYPE IF EXISTS struct;")
        print("CREATE TYPE struct AS (\n{}\n);".format(cols))
        return "struct"
