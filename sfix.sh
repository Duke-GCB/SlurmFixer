#!/usr/bin/env bash
ARGS="$*"
scl enable python33 - << EOF
    if [ ! -d 'env' ]
    then
        virtualenv env
        source env/bin/activate
        pip install -r requirements.txt
        deactivate
    fi
    source env/bin/activate
    python slurmfixer.py $ARGS
    deactivate
EOF
