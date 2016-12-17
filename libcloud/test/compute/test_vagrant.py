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
from __future__ import unicode_literals, print_function

import sys
import unittest

from libcloud.compute.base import NodeImage, Node
from libcloud.compute.drivers.vagrant import VagrantDriver
from .. import LibcloudTestCase


class VagrantMockResponses(object):
    def list_images(self):
        return ['cloudfoundry/bosh-lite', 'concourse/lite', 'eucalyptus-fullstack-2016-09-01', 'precise64']

    def list_sizes(self):
        raise NotImplementedError('Size is more-or-less arbitrary, so not applicable for this driver')


class VagrantDriverTestCase(LibcloudTestCase):
    vagrantfile_location = '/mnt/large_linux/vagrant/edx-fullstack'.encode('ascii')

    def setUp(self):
        self.driver = VagrantDriver(key='None'.encode('utf8'),
                                    ex_vagrantfile=self.vagrantfile_location)

    def test_list_images(self):
        print('self.driver.list_images(vagrantfile_location) =',
              self.driver.list_images(ex_vagrantfile=self.vagrantfile_location))
        for image in self.driver.list_images(ex_vagrantfile=self.vagrantfile_location):
            self.assertIsInstance(image, NodeImage)

    def test_list_nodes(self):
        print("self.driver.list_nodes(vagrantfile_location) =",
              self.driver.list_nodes(ex_vagrantfile=self.vagrantfile_location))

        for node in self.driver.list_nodes(ex_vagrantfile=self.vagrantfile_location):
            self.assertIsInstance(node, Node)


if __name__ == '__main__':
    sys.exit(unittest.main())
