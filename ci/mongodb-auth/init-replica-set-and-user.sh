#!/usr/bin/env bash

set -euo pipefail

readonly mongodb_uri='mongodb://root:risingwave@mongodb-auth:27017/admin?directConnection=true'

mongosh --quiet "$mongodb_uri" --eval "
    rs.initiate({
        _id: 'rs-auth',
        members: [{ _id: 0, host: 'mongodb-auth:27017' }]
    })
"

for _ in $(seq 1 60); do
    if mongosh --quiet "$mongodb_uri" --eval \
        'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
mongosh --quiet "$mongodb_uri" --eval \
    'quit(db.hello().isWritablePrimary ? 0 : 1)'

mongosh --quiet "$mongodb_uri" <<'EOF'
const admin = db.getSiblingDB('admin');
admin.createRole({
    role: 'rwListDatabases',
    privileges: [
        { resource: { cluster: true }, actions: ['listDatabases'] },
        { resource: { db: '', collection: '' }, actions: ['find', 'changeStream'] }
    ],
    roles: []
});
admin.createUser({
    user: 'rw_limited',
    pwd: 'risingwave',
    roles: [
        { role: 'rwListDatabases', db: 'admin' },
        { role: 'read', db: 'admin' },
        { role: 'read', db: 'rw_allowed' }
    ]
});
db.getSiblingDB('rw_allowed').events.insertOne({ _id: 1, value: 'snapshot' });
EOF
