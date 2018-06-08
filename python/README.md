[![Build Status](https://travis-ci.org/mozilla/mozdata.svg?branch=master)](https://travis-ci.org/mozilla/mozdata)

# MozData

A consistent API for accessing Mozilla data that reports usage to Mozilla

## Installing MozData for python

```bash
pip install mozdata
```

## Testing

Build the scala jar then run tests with tox

```
(cd .. && sbt assembly)
pip install tox
tox -c python/tox.ini
```
