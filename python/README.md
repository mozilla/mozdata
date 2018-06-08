[![Build Status](https://travis-ci.org/mozilla/mozdata.svg?branch=master)](https://travis-ci.org/mozilla/mozdata)
[![codecov.io](https://codecov.io/github/mozilla/mozdata/coverage.svg?branch=master)](https://codecov.io/github/mozilla/mozdata?branch=master)

# MozData

A consistent API for accessing Mozilla data that reports usage to Mozilla

## Installing and using MozData for python

Install via pip:

```bash
pip install mozdata
```

Import and provide spark session to init

```python
from mozdata import MozData

MozData(spark).sql("SELECT 0").show()
```

## Testing

Run tests via
[CircleCI Local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)

```bash
# from the top level directory of the git repo
cd "$(git rev-parse --show-toplevel)"
# build for python 2.7 or 3.7
circleci build --job python27_test
circleci build --job python37_test
```

Or run tests via `py.test`

```bash
pip install .[dev] --user
py.test
```

# License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
