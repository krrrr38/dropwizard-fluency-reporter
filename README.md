# dropwizard-fluency-reporter

[Dropwizard Metrics](http://metrics.dropwizard.io/) meets [Fluentd](https://www.fluentd.org/).

## Setup

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.krrrr38/dropwizard-fluency-reporter/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.krrrr38%22%20dropwizard-fluency-reporter)

```xml
<dependency>
  <groupId>com.krrrr38</groupId>
  <artifactId>dropwizard-fluency-reporter</artifactId>
  <version>${version}</version>
</dependency>
```

## Usage

- Send metrics to fluentd every minute.

```java
MetricRegistry metricRegistry = new MetricRegistry();
// ... metricRegistry.histogram("myhistogram").update(100)
Fluency fluency = Fluency.defaultFluency();
FluencyReporter.forRegistry(metricRegistry)
               .prefixedWith("mymetrics")
               .build(fluency)
               .start(1, TimeUnit.MINUTES);
```

- e.g. `mymetrics.myhistogram` json value

```json
{
  "min": 0,
  "max": 99,
  "count": 100,
  "mean": 49.50000000000002,
  "stddev": 28.866070047722125,
  "p50": 49.0,
  "p75": 74.0,
  "p95": 94.0,
  "p98": 97.0,
  "p99": 98.0,
  "p999": 99.0
}
```

## Dev Tools

### Release

```sh
make release
```

### SNAPSHOT Release

```sh
make snapshot
```

#### Tips

- raise `Inappropriate ioctl for device`, when snapshot release, try following

```
export GPG_TTY=$(tty)
```
