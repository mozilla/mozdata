# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import os
import os.path


def test_list_global_tables(api):
    assert not api.list_tables()
    api.spark.sql("SELECT 0").createOrReplaceTempView("table1")
    api.spark.sql("SELECT 0").createOrReplaceTempView("table2")
    assert(sorted(api.list_tables()) == ["table1", "table2"])


def test_list_ad_hoc_tables(api):
    owner = "list_tables"
    # make sure tables don't exist
    assert(not api.list_tables(owner=owner))
    # create tables
    os.makedirs(os.path.join(api.ad_hoc_tables_dir, owner, "table1", "v1"))
    os.makedirs(os.path.join(api.ad_hoc_tables_dir, owner, "table2", "1"))
    assert(sorted(api.list_tables(owner=owner)) == ["table1"])
