#!/bin/bash

# hash the password
RABBITMQ_HPSWD=$(echo -n "$RABBITMQ_UHPSWD" | openssl dgst -sha256)
RABBITMQ_HPSWD=${RABBITMQ_HPSWD#*(stdin)= }

# export the hashed password as an environment variable
export RABBITMQ_HPSWD

# unset the RABBITMQ_UHPSWD environment variable
unset RABBITMQ_UHPSWD

printenv

# start the AKHQ application
./akhq