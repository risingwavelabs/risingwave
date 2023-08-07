import json
import click
import user

@click.command()
@click.option('--num-users', default=10, help='Number of users to be generated')
@click.option('--dump-users', default='./users.json', help="The location to dump the `user` json file")
def generate(num_users, dump_users):
    """Simple program that greets NAME for a total of COUNT times."""
    users = [user.new_user() for _ in range(num_users)]
    json.dump(users, open(dump_users, 'w'), indent=2)


if __name__ == '__main__':
    generate()
