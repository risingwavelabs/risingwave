#!/bin/sh
set -e

# Copy the key from the mount to a location where we can change permissions
cp /etc/postgresql/server.key.mount /etc/postgresql/server.key

# Set correct permissions and ownership
chmod 600 /etc/postgresql/server.key
chown postgres:postgres /etc/postgresql/server.key

# Execute the original entrypoint
exec /usr/local/bin/docker-entrypoint.sh "$@"
