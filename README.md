# VMware-Go-KCL

The **VMware Kinesis Client Library for GO** (VMware-Go-KCL) enables Go developers to easily consume and process data from [Amazon Kinesis][kinesis].

It is a re-implementation on Amazon's Kinesis Client Library in pure Go without using KCL's multi-language support while keeping the same interface of original Aamzon's KCL for easy migration.

# Getting Started

## Pre-requisites

- Install [docker](https://www.docker.com)
- Install [HyperMake](https://evo-cloud.github.io/hmake)

Make sure hmake is version is 1.3.1 or above

```sh
hmake --version
1.3.1
```

Make sure to launch Docker daemon with specified DNS server `--dns DNS-SERVER-IP`

On Ubuntu, update the file `/etc/default/docker` to put `--dns DNS-SERVER-IP` in `DOCKER_OPTS`.

On Mac, set DNS in _Docker Preferences_ – _Daemon_ – _Insecure registries_

## Let's Go

```sh
hmake
# security scan
hmake scanast
# run test
hmake check
hmake test
```
