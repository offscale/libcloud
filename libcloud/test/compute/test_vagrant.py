# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import unicode_literals

import sys
import unittest

from libcloud.compute.drivers.hashicorp_vagrant import VagrantDriver
from .. import LibcloudTestCase


class VagrantMockResponses(object):
    def list_images(self):
        return ['cloudfoundry/bosh-lite', 'concourse/lite', 'eucalyptus-fullstack-2016-09-01', 'precise64']

    def list_sizes(self):
        raise NotImplementedError('Size is more-or-less arbitrary, so not applicable for this driver')


class VagrantDriverTestCase(LibcloudTestCase):
    def setUp(self):
        self.driver = VagrantDriver('None'.encode('utf8'))

    def test_list_images(self):
        print(self.driver.list_images())

    def test_list_nodes(self):
        print(self.driver.list_nodes('/mnt/large_linux/vagrant/edx-fullstack'))


if __name__ == '__main__':
    sys.exit(unittest.main())
