#!/usr/bin/env bash

project_dir=$(dirname $(dirname $0))

pass_file=$project_dir/env/.htpasswd
key_file=$project_dir/env/ssl.key
cert_file=$project_dir/env/ssl.crt

source $project_dir/.env

if [ -z "$BASIC_AUTH_USERNAME" ] || [ -z "$BASIC_AUTH_PASSWORD" ]; then
    echo "BASIC_AUTH_USERNAME and BASIC_AUTH_PASSWORD are required"
    exit 1
fi

if [ ! -d $project_dir/env ]; then
    mkdir $project_dir/env
fi

if [ ! -f $pass_file ]; then
    echo "Generating .htpasswd file..."
    echo "${BASIC_AUTH_USERNAME}:$(openssl passwd -apr1 $BASIC_AUTH_PASSWORD)" >$pass_file
else
    echo ".htpasswd file already exists"
fi

if [ ! -f $key_file ] || [ ! -f $cert_file ]; then
    echo "Generating self-signed certificate..."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout $key_file -out $cert_file -subj "/"
else
    echo "Certificate files already exist"
fi
