#!/bin/bash
. support/scripts/functions.sh

# Run only the unit tests and not integration tests
go test -race $(local_go_pkgs)
