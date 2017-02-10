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
from __future__ import print_function, absolute_import

import string
import unittest
from collections import namedtuple
from functools import partial
from itertools import imap

from shutil import rmtree
from tempfile import gettempdir
from uuid import uuid4
from os import path, mkdir

import operator

from libcloud.compute.base import NodeImage, Node, NodeSize
from libcloud.compute.drivers.vagrant import VagrantNodeDriver
from libcloud.common.vagrant import obj_to_d, pp, RubyEmit
from libcloud.test import LibcloudTestCase
from libcloud.utils.py3 import ensure_string

from types import (DictType, ListType, TupleType, BooleanType, FloatType,
                   StringType, UnicodeType, IntType, NoneType, LongType)

normal_types = (DictType, ListType, TupleType, BooleanType, FloatType,
                StringType, UnicodeType, IntType, NoneType, LongType)

obj_to_d = lambda obj: obj if type(obj) is DictType \
    else {k: getattr(obj, k) for k in dir(obj) if not k.startswith('_')}


def node_to_dict(node):
    if not node: return node
    node_d = {attr: getattr(node, attr) for attr in dir(node)
              if not attr.startswith('__') and type(getattr(node, attr)) in normal_types
              and getattr(node, attr)}
    node_d[
        'driver'] = node.driver.__name__ if node.driver.__class__.__name__ == 'type' else node.driver.__class__.__name__

    if hasattr(node, 'extra') and node.extra:
        if 'network_interfaces' in node_d['extra'] and node_d['extra']['network_interfaces']:
            node_d['extra']['network_interfaces'] = [
                interface if type(interface) is DictType
                else {'name': interface.name, 'id': interface.id}
                for interface in node_d['extra']['network_interfaces']]
        node_d['extra'] = {k: v for k, v in node.extra.iteritems()
                           if k not in ('secret', 'key') and type(v) in normal_types}
    if hasattr(node, 'availability_zone'):
        node_d['availability_zone'] = obj_to_d(node.availability_zone)
    return node_d


class VagrantMockResponses(object):
    def list_images(self):
        return ['cloudfoundry/bosh-lite', 'concourse/lite', 'eucalyptus-fullstack-2016-09-01', 'precise64']

    def list_sizes(self):
        raise NotImplementedError('Size is more-or-less arbitrary, so not applicable for this driver')


class VagrantDriverTestCase(LibcloudTestCase):
    vagrantfile_location = ensure_string('/mnt/large_linux/vagrant/edx-fullstack')
    driver = None

    def setUp(self):
        self.driver = VagrantNodeDriver(key=None, ex_vagrantfile=self.vagrantfile_location)

    def test_list_images(self):
        for image in self.driver.list_images(ex_vagrantfile=self.vagrantfile_location):
            self.assertIsInstance(image, NodeImage)

    def test_list_nodes(self):
        for node in self.driver.list_nodes(ex_vagrantfile=self.vagrantfile_location):
            # pp(obj_to_d(node))
            self.assertIsInstance(node, Node)


class RubyBlockGenerator(LibcloudTestCase):
    def test_block_create(self):
        vbox = RubyEmit.create_block(name='config.vm.provider', func_args='"virtualbox"', args='v',
                                     body_lines=('v.hello = "world"', 'v.goodbye = "world"', 'v.goodbye = "world"'))
        #print(vbox)
        print(RubyEmit.merge_blocks(vbox, vbox))
        # print(create_block(name='config.vm.provider', func_args='"virtualbox"', args='v',
        #                   body_lines=('v.hello = "world"', 'v.goodbye = "world"', 'v.goodbye = "world"')))

        #print(RubyEmit.create_block(name='config.vm.define "node_name"', args='foo'))
        # print ('config.vm.hostname = "{}"'.format(name),
        # 'config.vm.define "{name}" do |foo|'.format(name=name), 'end')


class VagrantParserTestCase(LibcloudTestCase):
    vagrantfile_location = None
    vagrantfile_dir = None
    driver = None

    @classmethod
    def setUpClass(cls):
        cls.vagrantfile_dir = path.join(gettempdir(), uuid4().get_hex())
        mkdir(cls.vagrantfile_dir)
        cls.vagrantfile_location = path.join(cls.vagrantfile_dir, 'Vagrantfile')
        cls.driver = VagrantNodeDriver(key=None, ex_vagrantfile=cls.vagrantfile_location)

    @classmethod
    def tearDownClass(cls):
        print(cls.vagrantfile_location)
        cls.driver.destroy_node(cls.vagrantfile_location)
        #rmtree(cls.vagrantfile_dir)

    def test_0_create_node(self):
        self.driver.create_node(name='any-cluster-ubuntu/xenial64-f5f600fb3aaa41a784f1eee1b49a8076',
                                image=NodeImage(id='ubuntu/xenial64', driver=self.driver,
                                                name='ubuntu/xenial64'),
                                key=self.vagrantfile_location,
                                ex_vagrantfile=self.vagrantfile_location,
                                location=None, ex_no_up=True,
                                size=None, ex_no_provision=True,
                                extras={
                                    'blocks': [
                                        {
                                            'name': 'virtualbox',
                                            'first_lines': [
                                                'hello'
                                            ]
                                        }
                                    ]
                                })
        with open(self.vagrantfile_location, 'rt') as f:
            vagrantfile_content = f.read()
        print('vagrantfile_content =', vagrantfile_content)

    def test_1_vagrantfile_contents(self):
        with open(self.vagrantfile_location, 'rt') as f:
            vagrantfile_content = f.read()

        print(vagrantfile_content)
        TestTup = namedtuple('TestTup', ('var', 'op', 'init', 'want'))
        TestRes = namedtuple('TestRes', ('res', 'want'))

        tests = {
            'end': TestTup('end', operator.eq, False, want=True)
        }
        expect_uncommented = (('config.vm.box', 'startswith'),
                              ('config.vm.hostname', 'startswith'),
                              ('config.vm.define', 'startswith'),
                              ('config.vm.provider "virtualbox" do', 'startswith'))
        for k in expect_uncommented:
            tests['commented/nonexistent - {}'.format(k[0])] = TestTup(*k, init=False, want=True)

        test_results = {}
        for line in vagrantfile_content.splitlines():
            for test, test_tup in tests.iteritems():
                r = (lambda l: TestRes(test_tup.op(test_tup.var, l) if callable(test_tup.op)
                                       else getattr(l, test_tup.op)(test_tup.var),
                                       want=test_tup.want))(line.lstrip())
                if test not in test_results or r.res == r.want:
                    test_results[test] = r

        for test, test_tup in test_results.iteritems():
            self.assertEqual(test_tup.want, test_tup.res, msg=test)

    '''def comment_test_3_edit_vagrantfile(self):
        with open(self.vagrantfile_location, 'r+') as f:
            print(''.join(VagrantfileEditor.one_block_parse_emit(
                first_lines=('foo = 5', 'bar = 6'), memory=2048, cpus=4, provider='virtualbox')(f)))

        def test_1_parsed_vagrantobj(self):
            pass  # pp(self.driver.ex_parse_vagrantfile(self.vagrantfile_location).vm.to_dict())

        def test_2_parsed_vagrantobj(self):
            pass
            # pp(self.driver.ex_parse_vagrantfile('/mnt/large_linux/vagrant/edx-fullstack/Vagrantfile').to_dict())'''


if __name__ == '__main__':
    exit(unittest.main())
