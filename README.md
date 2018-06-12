[![Build Status](https://travis-ci.org/mozilla/mozdata.svg?branch=master)](https://travis-ci.org/mozilla/mozdata)
[![codecov.io](https://codecov.io/github/mozilla/mozdata/coverage.svg?branch=master)](https://codecov.io/github/mozilla/mozdata?branch=master)

# MozData

A consistent API for accessing Mozilla data that reports usage to Mozilla
```

## Using MozData in scala

In SBT:
```sbt
resolvers += "S3 local maven snapshots" at "s3://net-mozaws-data-us-west-2-ops-mavenrepo/snapshots"
libraryDependencies += "com.mozilla.telemetry" %% "mozdata" % "0.1-SNAPSHOT"

## Data Collection

This api sends usage data to telemetry when the env var `TELEMETRY_URL` is properly set.

On Mozilla servers `TELEMETRY_URL` should be set to `https://incoming.telemetry.mozilla.org`

To disable data collection in environment variables:
```bash
unset TELEMETRY_URL
```
or in scala:
```scala
import com.mozilla.telemetry.mozdata.MozData
val api: MozData = MozData(telemetryUrl=None)
```

To enable printing what you are or would be sending in log4j.properties (probably in src/main/resources or src/test/resources):
```properties
log4j.logger.com.mozilla.telemetry.mozdata.MozData=DEBUG
```
or in scala:
```scala
import org.apache.log4j.{Logger, Level}
Logger.getLogger(classOf[MozData]).setLevel(Level.DEBUG)
```

## Development

Run tests with sbt

```bash
sbt test scalastyle test:scalastyle
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
