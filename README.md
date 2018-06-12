[![Build Status](https://travis-ci.org/mozilla/mozdata.svg?branch=master)](https://travis-ci.org/mozilla/mozdata)
[![codecov.io](https://codecov.io/github/mozilla/mozdata/coverage.svg?branch=master)](https://codecov.io/github/mozilla/mozdata?branch=master)

# MozData

A consistent API for accessing Mozilla data that reports usage to Mozilla

## Using MozData in scala

In SBT:
```sbt
resolvers += "S3 local maven snapshots" at "s3://net-mozaws-data-us-west-2-ops-mavenrepo/snapshots"
libraryDependencies += "com.mozilla.telemetry" %% "mozdata" % "0.1-SNAPSHOT"
```

## Testing

Install moto then run tests with sbt

```bash
sbt test
sbt scalastyle
sbt test:scalastyle
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
