# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

# This file contains tests which check Scylla-specific features that do
# not exist on AWS. So all these tests are skipped when running with "--aws".

import pytest
import requests
import json

# Test that the "/localnodes" request works, returning at least the one node.
# TODO: A more through test would need to start a cluster with multiple nodes
# in multiple data centers, and check that we can get a list of nodes in each
# data center. But this test framework cannot yet test that.
def test_localnodes(scylla_only, dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url + '/localnodes', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) >= 1
