# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from collections import namedtuple
from mock import patch
from mozdata import MozData, __version__
import json
import logging
import pytest
import re

Log = namedtuple("Log", "level msg args kwargs")
Post = namedtuple("Post", "url data json kwargs")
uuid_pattern = re.compile(
    "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",
    re.IGNORECASE
)


@pytest.mark.parametrize("log_kwargs", [{}, {"meta": "data"}, {"drop": None}])
@pytest.mark.parametrize("mozdata_kwargs", [{}, {"telemetry_url": "prefix"}])
def test_log(log_kwargs, mozdata_kwargs, spark_fake):
    logs = []

    def debug(msg, *args, **kwargs):
        logs.append(Log(logging.DEBUG, msg, args, kwargs))

    posts = []

    def post(url, data=None, json=None, **kwargs):
        posts.append(Post(url, data, json, kwargs))

    logger = logging.getLogger("MozData")
    with patch.object(logger, "debug", debug), patch("requests.post", post):
        # make log call
        MozData(spark_fake, **mozdata_kwargs)._log("action", **log_kwargs)
        # validate logging
        assert len(logs) == 1
        assert logs[0].level == logging.DEBUG
        assert json.loads(logs[0].msg) == dict(
            apiVersion=__version__,
            apiCall="action",
            **(log_kwargs if list(log_kwargs.values()) != [None] else {})
        )
        assert logs[0].args == ()
        assert logs[0].kwargs == {}
        # validate telemetry
        if "telemetry_url" not in mozdata_kwargs:
            assert len(posts) == 0
        else:
            assert len(posts) == 1
            url = mozdata_kwargs["telemetry_url"] + "/submit/mozdata/event/1/"
            assert posts[0].url.startswith(url)
            uuid = posts[0].url[len(url):]
            assert len(uuid) == 36
            assert uuid_pattern.match(uuid)
            # validated contents of logs[0].msg above, check they match
            assert posts[0].data.decode() == logs[0].msg
            assert posts[0].json is None
            assert posts[0].kwargs == {
                "headers": {"content_type": "application/json"}
            }
