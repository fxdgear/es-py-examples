#!/bin/bash

COMMAND="$@"
docker run --rm -it -v `pwd`:/examples --network=host example:latest ${COMMAND}
