

# VMware-Go-KCL

## Overview

[Amazon Kinesis](https://aws.amazon.com/kinesis/data-streams/)  enables real-time processing of streaming data at massive scale. Kinesis Streams is useful for rapidly moving data off data producers and then continuously processing the data, be it to transform the data before emitting to a data store, run real-time metrics and analytics, or derive more complex data streams for further processing.

The **VMware Kinesis Client Library for GO** (VMware-Go-KCL) enables Go developers to easily consume and process data from [Amazon Kinesis][kinesis].

**VMware-Go-KCL** brings Go/Kubernetes community with Go language native implementation of KCL matching **exactly the same** API and functional spec of original [Java KCL v2.0](https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html) without the resource overhead of installing Java based MultiLangDaemon.


## Try it out

### Prerequisites

- Install [Go](https://golang.org/)
- Install [docker](https://www.docker.com)
- Install [HyperMake](https://evo-cloud.github.io/hmake)
- Config [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)

Make sure hmake is version is 1.3.1 or above

```sh
hmake --version
1.3.1
```

Make sure to launch Docker daemon with specified DNS server `--dns DNS-SERVER-IP`

On Ubuntu, update the file `/etc/default/docker` to put `--dns DNS-SERVER-IP` in `DOCKER_OPTS`.

On Mac, set DNS in _Docker Preferences_ – _Daemon_ – _Insecure registries_

### Build & Run

```sh
hmake

# security scan
hmake scanast

# run test
hmake check

# run integration test
# update the worker_test.go to let it point to your Kinesis stream
hmake test
```

## Documentation

VMware-Go-KCL matches exactly the same interface and programming model from original Amazon KCL, the best place for getting reference, tutorial is from Amazon itself:

- [Developing Consumers Using the Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
- [Troubleshooting](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
- [Advanced Topics](https://docs.aws.amazon.com/streams/latest/dev/advanced-consumers.html)

## Releases & Major Branches


## Contributing

The vmware-go-kcl project team welcomes contributions from the community. If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a Pull Request. For any
questions about the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq). For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License