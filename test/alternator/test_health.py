# Copyright 2019 ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

# Tests for the health check

import requests

# Test that a health check can be performed with a GET packet
def test_health_works(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url, verify=False)
    assert response.ok
    assert response.content.decode('utf-8').strip()  == 'healthy: {}'.format(url.replace('https://', '').replace('http://', ''))

# Test that a health check only works for the root URL ('/')
def test_health_only_works_for_root_path(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    for suffix in ['/abc', '/-', '/index.htm', '/health']:
        print(url + suffix)
        response = requests.get(url + suffix, verify=False)
        assert response.status_code in range(400, 405)
