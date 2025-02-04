# Runners autoscale configuration

> The autoscale feature was introduced in GitLab Runner 1.1.0.

Autoscale provides the ability to utilize resources in a more elastic and
dynamic way.

Thanks to Runners being able to autoscale, your infrastructure contains only as
much build instances as necessary at anytime. If you configure the Runner to
only use autoscale, the system on which the Runner is installed acts as a
bastion for all the machines it creates.

## Overview

When this feature is enabled and configured properly, jobs are executed on
machines created _on demand_. Those machines, after the job is finished, can
wait to run the next jobs or can be removed after the configured `IdleTime`.
In case of many cloud providers this helps to utilize the cost of already used
instances.

Below, you can see a real life example of the runners autoscale feature, tested
on GitLab.com for the [GitLab Community Edition][ce] project:

![Real life example of autoscaling](img/autoscale-example.png)

Each machine on the chart is an independent cloud instance, running jobs
inside of Docker containers.

[ce]: https://gitlab.com/gitlab-org/gitlab-ce

## System requirements

At this point you should have
[installed all the requirements](../executors/docker_machine.md#preparing-the-environment).
If not, make sure to do it before going over the configuration.

## Supported cloud providers

The autoscale mechanism is based on [Docker Machine](https://docs.docker.com/machine/overview/).
All supported virtualization/cloud provider parameters, are available at the
[Docker Machine drivers documentation](https://docs.docker.com/machine/drivers/).

## Runner configuration

In this section we will describe only the significant parameters from the
autoscale feature point of view. For more configurations details read the
[advanced configuration](advanced-configuration.md).

### Runner global options

| Parameter    | Value   | Description |
|--------------|---------|-------------|
| `concurrent` | integer | Limits how many jobs globally can be run concurrently. This is the most upper limit of number of jobs using _all_ defined runners, local and autoscale. Together with `limit` (from [`[[runners]]` section](#runners-options)) and `IdleCount` (from [`[runners.machine]` section][runners-machine]) it affects the upper limit of created machines. |

### `[[runners]]` options

| Parameter  | Value            | Description |
|------------|------------------|-------------|
| `executor` | string           | To use the autoscale feature, `executor` must be set to `docker+machine` or `docker-ssh+machine`. |
| `limit`    | integer          | Limits how many jobs can be handled concurrently by this specific token. 0 simply means don't limit. For autoscale it's the upper limit of machines created by this provider (in conjunction with `concurrent` and `IdleCount`). |

### `[runners.machine]` options

Configuration parameters details can be found
in [GitLab Runner - Advanced Configuration - The `[runners.machine]` section][runners-machine].

### `[runners.cache]` options

Configuration parameters details can be found
in [GitLab Runner - Advanced Configuration - The `[runners.cache]` section][runners-cache]

### Additional configuration information

There is also a special mode, when you set `IdleCount = 0`. In this mode,
machines are **always** created **on-demand** before each job (if there is no
available machine in _Idle_ state). After the job is finished, the autoscaling
algorithm works
[the same as it is described below](#autoscaling-algorithm-and-parameters).
The machine is waiting for the next jobs, and if no one is executed, after
the `IdleTime` period, the machine is removed. If there are no jobs, there
are no machines in _Idle_ state.

## Autoscaling algorithm and parameters

The autoscaling algorithm is based on three main parameters: `IdleCount`,
`IdleTime` and `limit`.

We say that each machine that does not run a job is in _Idle_ state. When
GitLab Runner is in autoscale mode, it monitors all machines and ensures that
there is always an `IdleCount` of machines in _Idle_ state.

At the same time, GitLab Runner is checking the duration of the _Idle_ state of
each machine. If the time exceeds the `IdleTime` value, the machine is
automatically removed.

---

**Example:**
Let's suppose, that we have configured GitLab Runner with the following
autoscale parameters:

```bash
[[runners]]
  limit = 10
  (...)
  executor = "docker+machine"
  [runners.machine]
    IdleCount = 2
    IdleTime = 1800
    (...)
```

At the beginning, when no jobs are queued, GitLab Runner starts two machines
(`IdleCount = 2`), and sets them in _Idle_ state. Notice that we have also set
`IdleTime` to 30 minutes (`IdleTime = 1800`).

Now, let's assume that 5 jobs are queued in GitLab CI. The first 2 jobs are
sent to the _Idle_ machines of which we have two. GitLab Runner now notices that
the number of _Idle_ is less than `IdleCount` (`0 < 2`), so it starts 2 new
machines. Then, the next 2 jobs from the queue are sent to those newly created
machines. Again, the number of _Idle_ machines is less than `IdleCount`, so
GitLab Runner starts 2 new machines and the last queued job is sent to one of
the _Idle_ machines.

We now have 1 _Idle_ machine, so GitLab Runner starts another 1 new machine to
satisfy `IdleCount`. Because there are no new jobs in queue, those two
machines stay in _Idle_ state and GitLab Runner is satisfied.

---

**This is what happened:**
We had 2 machines, waiting in _Idle_ state for new jobs. After the 5 jobs
where queued, new machines were created, so in total we had 7 machines. Five of
them were running jobs, and 2 were in _Idle_ state, waiting for the next
jobs.

The algorithm will still work in the same way; GitLab Runner will create a new
_Idle_ machine for each machine used for the job execution until `IdleCount`
is satisfied. Those machines will be created up to the number defined by
`limit` parameter. If GitLab Runner notices that there is a `limit` number of
total created machines, it will stop autoscaling, and new jobs will need to
wait in the job queue until machines start returning to _Idle_ state.

In the above example we will always have two idle machines. The `IdleTime`
applies only when we are over the `IdleCount`, then we try to reduce the number
of machines to `IdleCount`.

---

**Scaling down:**
After the job is finished, the machine is set to _Idle_ state and is waiting
for the next jobs to be executed. Let's suppose that we have no new jobs in
the queue. After the time designated by `IdleTime` passes, the _Idle_ machines
will be removed. In our example, after 30 minutes, all machines will be removed
(each machine after 30 minutes from when last job execution ended) and GitLab
Runner will start to keep an `IdleCount` of _Idle_ machines running, just like
at the beginning of the example.

---

So, to sum up:

1. We start the Runner
2. Runner creates 2 idle machines
3. Runner picks one job
4. Runner creates one more machine to fulfill the strong requirement of always
   having the two idle machines
5. Job finishes, we have 3 idle machines
6. When one of the three idle machines goes over `IdleTime` from the time when
   last time it picked the job it will be removed
7. The Runner will always have at least 2 idle machines waiting for fast
   picking of the jobs

Below you can see a comparison chart of jobs statuses and machines statuses
in time:

![Autoscale state chart](img/autoscale-state-chart.png)

## How `concurrent`, `limit` and `IdleCount` generate the upper limit of running machines

There doesn't exist a magic equation that will tell you what to set `limit` or
`concurrent` to. Act according to your needs. Having `IdleCount` of _Idle_
machines is a speedup feature. You don't need to wait 10s/20s/30s for the
instance to be created. But as a user, you'd want all your machines (for which
you need to pay) to be running jobs, not stay in _Idle_ state. So you should
have `concurrent` and `limit` set to values that will run the maximum count of
machines you are willing to pay for. As for `IdleCount`, it should be set to a
value that will generate a minimum amount of _not used_ machines when the job
queue is empty.

Let's assume the following example:

```bash
concurrent=20

[[runners]]
  limit = 40
  [runners.machine]
    IdleCount = 10
```

In the above scenario the total amount of machines we could have is 30. The
`limit` of total machines (building and idle) can be 40. We can have 10 idle
machines but the `concurrent` jobs are 20. So in total we can have 20
concurrent machines running jobs and 10 idle, summing up to 30.

But what happens if the `limit` is less than the total amount of machines that
could be created? The example below explains that case:

```bash
concurrent=20

[[runners]]
  limit = 25
  [runners.machine]
    IdleCount = 10
```

In this example we will have at most 20 concurrent jobs, and at most 25
machines created. In the worst case scenario regarding idle machines, we will
not be able to have 10 idle machines, but only 5, because the `limit` is 25.

## Off Peak time mode configuration

> Introduced in GitLab Runner v1.7

Autoscale can be configured with the support for _Off Peak_ time mode periods.

**What is _Off Peak_ time mode period?**

Some organizations can select a regular time periods when no work is done.
For example most of commercial companies are working from Monday to
Friday in a fixed hours, eg. from 10am to 6pm. In the rest of the week -
from Monday to Friday at 12am-9am and 6pm-11pm and whole Saturday and Sunday -
no one is working. These time periods we're naming here as _Off Peak_.

Organizations where _Off Peak_ time periods occurs probably don't want
to pay for the _Idle_ machines when it's certain that no jobs will be
executed in this time. Especially when `IdleCount` is set to a big number.

In the `v1.7` version of the Runner we've added the support for _Off Peak_
configuration. With parameters described in configuration file you can now
change the `IdleCount` and `IdleTime` values for the _Off Peak_ time mode
periods.

**How it is working?**

Configuration of _Off Peak_ is done by four parameters: `OffPeakPeriods`,
`OffPeakTimezone`, `OffPeakIdleCount` and `OffPeakIdleTime`. The
`OffPeakPeriods` setting contains an array of cron-style patterns defining
when the _Off Peak_ time mode should be set on. For example:

```toml
[runners.machine]
  OffPeakPeriods = [
    "* * 0-9,18-23 * * mon-fri *",
    "* * * * * sat,sun *"
  ]
```

will enable the _Off Peak_ periods described above, so the _working_ days
from 12am to 9am and from 6pm to 11pm and whole weekend days. Machines
scheduler is checking all patterns from the array and if at least one of
them describes current time, then the _Off Peak_ time mode is enabled.

You can specify the `OffPeakTimezone` e.g. `"Australia/Sydney"`. If you don't,
the system setting of the host machine of every runner will be used. This
default can be stated as `OffPeakTimezone = "Local"` explicitly if you wish.

When the _Off Peak_ time mode is enabled machines scheduler use
`OffPeakIdleCount` instead of `IdleCount` setting and `OffPeakIdleTime`
instead of `IdleTime` setting. The autoscaling algorithm is not changed,
only the parameters. When machines scheduler discovers that none from
the `OffPeakPeriods` pattern is fulfilled then it switches back to
`IdleCount` and `IdleTime` settings.

More information about syntax of `OffPeakPeriods` patterns can be found
in [GitLab Runner - Advanced Configuration - The `[runners.machine]` section][runners-machine].

## Distributed runners caching

NOTE: **Note:**
Read how to [install your own cache server](../install/registry_and_cache_servers.md#install-your-own-cache-server).

To speed up your jobs, GitLab Runner provides a [cache mechanism][cache]
where selected directories and/or files are saved and shared between subsequent
jobs.

This is working fine when jobs are run on the same host, but when you start
using the Runners autoscale feature, most of your jobs will be running on a
new (or almost new) host, which will execute each job in a new Docker
container. In that case, you will not be able to take advantage of the cache
feature.

To overcome this issue, together with the autoscale feature, the distributed
Runners cache feature was introduced.

It uses configured object storage server to share the cache between used Docker hosts.
When restoring and archiving the cache, GitLab Runner will query the server
and will download or upload the archive respectively.

To enable distributed caching, you have to define it in `config.toml` using the
[`[runners.cache]` directive][runners-cache]:

```bash
[[runners]]
  limit = 10
  executor = "docker+machine"
  [runners.cache]
    Type = "s3"
    Path = "path/to/prefix"
    Shared = false
    [runners.cache.s3]
      ServerAddress = "s3.example.com"
      AccessKey = "access-key"
      SecretKey = "secret-key"
      BucketName = "runner"
      Insecure = false
```

In the example above, the S3 URLs follow the structure
`http(s)://<ServerAddress>/<BucketName>/<Path>/runner/<runner-id>/project/<id>/<cache-key>`.

To share the cache between two or more Runners, set the `Shared` flag to true.
That will remove the runner token from the URL (`runner/<runner-id>`) and
all configured Runners will share the same cache. Remember that you can also
set `Path` to separate caches between Runners when cache sharing is enabled.

## Distributed container registry mirroring

NOTE: **Note:**
Read how to [install a container registry](../install/registry_and_cache_servers.md#install-a-proxy-container-registry).

To speed up jobs executed inside of Docker containers, you can use the [Docker
registry mirroring service][registry]. This will provide a proxy between your
Docker machines and all used registries. Images will be downloaded once by the
registry mirror. On each new host, or on an existing host where the image is
not available, it will be downloaded from the configured registry mirror.

Provided that the mirror will exist in your Docker machines LAN, the image
downloading step should be much faster on each host.

To configure the Docker registry mirroring, you have to add `MachineOptions` to
the configuration in `config.toml`:

```bash
[[runners]]
  limit = 10
  executor = "docker+machine"
  [runners.machine]
    (...)
    MachineOptions = [
      (...)
      "engine-registry-mirror=http://10.11.12.13:12345"
    ]
```

Where `10.11.12.13:12345` is the IP address and port where your registry mirror
is listening for connections from the Docker service. It must be accessible for
each host created by Docker Machine.

## A complete example of `config.toml`

The `config.toml` below uses the [`digitalocean` Docker Machine driver](https://docs.docker.com/machine/drivers/digital-ocean/):

```bash
concurrent = 50   # All registered Runners can run up to 50 concurrent jobs

[[runners]]
  url = "https://gitlab.com"
  token = "RUNNER_TOKEN"             # Note this is different from the registration token used by `gitlab-runner register`
  name = "autoscale-runner"
  executor = "docker+machine"        # This Runner is using the 'docker+machine' executor
  limit = 10                         # This Runner can execute up to 10 jobs (created machines)
  [runners.docker]
    image = "ruby:2.1"               # The default image used for jobs is 'ruby:2.1'
  [runners.machine]
    OffPeakPeriods = [               # Set the Off Peak time mode on for:
      "* * 0-9,18-23 * * mon-fri *", # - Monday to Friday for 12am to 9am and 6pm to 11pm
      "* * * * * sat,sun *"          # - whole Saturday and Sunday
    ]
    OffPeakIdleCount = 1             # There must be 1 machine in Idle state - when Off Peak time mode is on
    OffPeakIdleTime = 1200           # Each machine can be in Idle state up to 1200 seconds (after this it will be removed) - when Off Peak time mode is on
    IdleCount = 5                    # There must be 5 machines in Idle state - when Off Peak time mode is off
    IdleTime = 600                   # Each machine can be in Idle state up to 600 seconds (after this it will be removed) - when Off Peak time mode is off
    MaxBuilds = 100                  # Each machine can handle up to 100 jobs in a row (after this it will be removed)
    MachineName = "auto-scale-%s"    # Each machine will have a unique name ('%s' is required)
    MachineDriver = "digitalocean"   # Docker Machine is using the 'digitalocean' driver
    MachineOptions = [
        "digitalocean-image=coreos-stable",
        "digitalocean-ssh-user=core",
        "digitalocean-access-token=DO_ACCESS_TOKEN",
        "digitalocean-region=nyc2",
        "digitalocean-size=4gb",
        "digitalocean-private-networking",
        "engine-registry-mirror=http://10.11.12.13:12345"   # Docker Machine is using registry mirroring
    ]
  [runners.cache]
    Type = "s3"
    [runners.cache.s3]
      ServerAddress = "s3-eu-west-1.amazonaws.com"
      AccessKey = "AMAZON_S3_ACCESS_KEY"
      SecretKey = "AMAZON_S3_SECRET_KEY"
      BucketName = "runner"
      Insecure = false
```

Note that the `MachineOptions` parameter contains options for the `digitalocean`
driver which is used by Docker Machine to spawn machines hosted on Digital Ocean,
and one option for Docker Machine itself (`engine-registry-mirror`).

[cache]: https://docs.gitlab.com/ee/ci/yaml/README.html#cache
[docker-machine-docs]: https://docs.docker.com/machine/
[docker-machine-driver]: https://docs.docker.com/machine/drivers/
[docker-machine-installation]: https://docs.docker.com/machine/install-machine/
[runners-cache]: advanced-configuration.md#the-runnerscache-section
[runners-machine]: advanced-configuration.md#the-runnersmachine-section
[registry]: https://docs.docker.com/registry/
