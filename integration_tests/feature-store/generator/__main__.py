import json
import click
import user
    # This is a package in preview.
from azureml.opendatasets import NycTlcGreen

from datetime import datetime
from dateutil import parser

@click.command()
@click.option('--types', default='taxi', help='taxi or mfa')
@click.option('--num-users', default=10, help='Number of users to be generated')
@click.option('--dump-users', default='./users.json', help="The location to dump the `user` json file")
def generate(num_users, dump_users,types):
    if types == 'taxi':
        get_data_from_azure()
    else:
        users = [user.new_user() for _ in range(num_users)]
        json.dump(users, open(dump_users, 'w'), indent=2)

def get_data_from_azure():
    end_date = parser.parse('2018-05-02')
    start_date = parser.parse('2018-05-01')
    print("Getting data from {} to {}".format(start_date, end_date))
    nyc_tlc = NycTlcGreen(start_date=start_date, end_date=end_date)
    nyc_tlc_df = nyc_tlc.to_pandas_dataframe()
    nyc_tlc_df.iloc[0:10000].to_csv('/opt/feature-store/parquet_data.csv')

if __name__ == '__main__':
    generate()
