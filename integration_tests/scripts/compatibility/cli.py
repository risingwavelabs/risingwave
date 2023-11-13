import click
from compatibility import *


@click.group()
def cli():
    pass


@click.command()
@click.option("--datatype-file", default="./compatibility/risingwave-datatypes.yml", help="data type file")
@click.option("--database-type", default="postgres", help="database type")
def gen_select_sql(datatype_file: str, database_type: str):
    database_type = database_type.lower()
    with open(datatype_file) as f:
        datatypes_map = yaml.safe_load(f)
        datatype_list = []
        for data_type in datatypes_map["datatypes"]:
            new_datatype = DataType(**data_type)
            if database_type == "mysql":
                new_datatype = MysqlDataType(**data_type)
            datatype_list.append(new_datatype)
            print(new_datatype.select_zero_sql())
            print(new_datatype.select_min_sql())
            print(new_datatype.select_max_sql())
            if data_type in ["postgres", "risingwave"]:
                print(new_datatype.select_array_zero_sql())
                print(new_datatype.select_array_min_sql())
                print(new_datatype.select_array_max_sql())


@click.command()
@click.option("--datatype-file", default="./compatibility/risingwave-datatypes.yml", help="data type file")
@click.option("--database-type", default="postgres", help="database type")
def gen_ddl_dml(datatype_file: str, database_type: str):
    database_type = database_type.lower()
    with open(datatype_file) as f:
        datatypes_map = yaml.safe_load(f)
        datatype_list = []
        for data_type in datatypes_map["datatypes"]:
            new_datatype = DataType(**data_type)
            if database_type == "mysql":
                new_datatype = MysqlDataType(**data_type)
            datatype_list.append(new_datatype)
        table_sql_generator = TableSqlGenerator(
            name='{}_all_types'.format(database_type),
            enable_array=False,
            enable_struct=False,
            pk_types=datatypes_map.get("pk_types", []),
            datatypes=datatype_list
        )
        if database_type == "mysql":
            pass
        elif database_type == "postgres":
            table_sql_generator = PostgresTableSqlGenerator(
                name='{}_all_types'.format(database_type),
                enable_array=True,
                enable_struct=False,
                pk_types=datatypes_map.get("pk_types", []),
                datatypes=datatype_list
            )
        elif database_type == "risingwave":
            table_sql_generator = RisingwaveTableSqlGenerator(
                name='{}_all_types'.format(database_type),
                enable_array=True,
                enable_struct=True,
                pk_types=datatypes_map.get("pk_types", []),
                datatypes=datatype_list
            )

        print(table_sql_generator.drop_table_sql())
        print(table_sql_generator.create_table_sql())
        print(table_sql_generator.insert_zero_sql())
        print(table_sql_generator.insert_min_sql())
        print(table_sql_generator.insert_max_sql())


cli.add_command(gen_select_sql)
cli.add_command(gen_ddl_dml)

if __name__ == '__main__':
    cli()
