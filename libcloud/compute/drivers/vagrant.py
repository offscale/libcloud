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

"""
Node driver for Vagrant.
"""
from __future__ import print_function
import subprocess
from functools import partial
from itertools import imap
from os import path, getcwd, mkdir, remove, rename
import logging
from sys import stdout, stderr

from libcloud.common.types import LibcloudError
from libcloud.common.vagrant import (Vagrant, get_vagrant_executable, StreamLogger,
                                     LevelRangeFilter, pp, isIpPrivate, VagrantParser, Vagrantfile, VagrantfileEditor,
                                     Virtualbox, add_to)

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
try:
    import simplejson as json
except ImportError:
    import json

from libcloud.compute.base import Node, NodeDriver, NodeImage, NodeSize
from libcloud.compute.types import NodeState, StorageVolumeState, VolumeSnapshotState

log = logging.getLogger(__name__)


class VagrantNodeDriver(NodeDriver):
    """
    Vagrant node driver.

    @inherits :class:`NodeDriver`
    """

    name = 'Vagrant'
    website = 'https://www.vagrantup.com'
    features = {'create_node': ['ssh_key']}

    NODE_STATE_MAPPING = {
        'Starting': NodeState.PENDING,
        'Running': NodeState.RUNNING,
        'Stopping': NodeState.PENDING,
        'Stopped': NodeState.STOPPED
    }

    VOLUME_STATE_MAPPING = {
        'In_use': StorageVolumeState.INUSE,
        'Available': StorageVolumeState.AVAILABLE,
        'Attaching': StorageVolumeState.ATTACHING,
        'Detaching': StorageVolumeState.INUSE,
        'Creating': StorageVolumeState.CREATING,
        'ReIniting': StorageVolumeState.CREATING}

    SNAPSHOT_STATE_MAPPING = {
        'progressing': VolumeSnapshotState.CREATING,
        'accomplished': VolumeSnapshotState.AVAILABLE,
        'failed': VolumeSnapshotState.ERROR}

    def __init__(self, ex_vagrantfile=None, *args, **kwargs):
        """
        Instantiate VagrantDriver object

        @inherits: :class:`NodeDriver.__init__` (`BaseDriver.__init__`)

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                 default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``list`` of ``str`` or ``str``
        """
        if 'key' not in kwargs:
            raise TypeError('base class requires `key` be defined')
        super(self.__class__, self).__init__(ex_vagrantfile=ex_vagrantfile, *args, **kwargs)
        self.vagrants = Vagrantfiles([ex_vagrantfile] if isinstance(ex_vagrantfile, basestring) else ex_vagrantfile)
        self.last_node_name = None

    def list_nodes(self, ex_vagrantfile=None, ex_vm_name=None, ex_provider=None):
        """
        List all nodes.

        @inherits: :class:`NodeDriver.list_nodes`

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [global]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: required in a multi-VM environment.
        :type ex_vm_name: ``str``

        :return: list of Node
        :rtype: ``list`` of ``Node``
        """

        def get_vagrant_obj(ex_vagrantfile, globalStatus=None):
            """
            :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [global]. If a filepath is given, its dir is resolved.
            :type ex_vagrantfile: ``str`` or ``None``

            :keyword globalStatus: GlobalStatus object default to ``None``.
            :type globalStatus: ``GlobalStatus``
            """
            if ex_vagrantfile is None and globalStatus is not None:
                ex_vagrantfile = ex_vagrantfile

            _stderr_stream = logging.StreamHandler(StringIO())
            _stderr_stream.addFilter(LevelRangeFilter(logging.ERROR, None))
            log.addHandler(_stderr_stream)
            log.setLevel(logging.ERROR)

            _stdout_stream = logging.StreamHandler(StringIO())
            _stdout_stream.addFilter(LevelRangeFilter(logging.INFO, logging.ERROR))
            log.addHandler(_stdout_stream)
            log.setLevel(logging.INFO)

            self.vagrants.push(ex_vagrantfile,
                               err_cm=StreamLogger(log, logging.ERROR),
                               out_cm=StreamLogger(log, logging.INFO))
            return self.vagrants[ex_vagrantfile], _stderr_stream, _stdout_stream

        def get_node(globalStatus):
            """
            :keyword globalStatus: GlobalStatus object
            :type globalStatus: ``GlobalStatus``
            """
            _vagrant, _stderr_stream, _stdout_stream = get_vagrant_obj(None, globalStatus)

            try:
                return self.ex_parse_node(_vagrant, globalStatus, vm_name=globalStatus.id,
                                          node_name=(globalStatus.name
                                                     if globalStatus.name != 'default' or not self.last_node_name
                                                     else self.last_node_name))
            except subprocess.CalledProcessError as e:
                log.exception(LibcloudError(value='CalledProcessError(cmd={e.cmd}, args={e.args},'
                                                  ' returncode={e.returncode}, stdout={stdout!r},'
                                                  ' stderr={stderr!r})'.format(e=e,
                                                                               stderr=_stderr_stream.stream.getvalue(),
                                                                               stdout=_stdout_stream.stream.getvalue()),
                                            driver=VagrantNodeDriver))
                '''if ex_vagrantfile:
                    extra = dict(ex_vagrantfile=ex_vagrantfile)
                else:
                    extra = Vagrant().status(vm_name=globalStatus.id)'''

                return Node(id=globalStatus.id, name=ex_vagrantfile, state='poweroff',
                            extra=dict(ex_vagrantfile=ex_vagrantfile),
                            driver=VagrantNodeDriver, private_ips=[], public_ips=[])
            finally:
                _vagrant.err_cm.close()
                _vagrant.out_cm.close()

        vagrant, stderr_stream, stdout_stream = get_vagrant_obj(ex_vagrantfile)
        try:
            return [get_node(globalStatus)
                    for globalStatus in vagrant.global_status()]
        finally:
            vagrant.err_cm.close()
            vagrant.out_cm.close()

    def list_sizes(self, location=None, ex_vagrantfile=None):
        """
        @inherits :class:`NodeDriver.list_sizes`

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword location:
        :rtype ``list`` of ``NodeSize``
        """
        raise NotImplementedError('N/A for Vagrant')

    def list_locations(self, ex_vagrantfile=None):
        """
        @inherits :class:`NodeDriver.list_locations`

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :rtype ``list`` of ``NodeSize``
        """
        raise NotImplementedError('N/A for Vagrant')

    def create_node(self, name, size, image=None, auth=None, ex_vagrantfile=None, ex_box_url=None,
                    ex_no_provision=False, ex_provider='virtualbox', ex_vm_name=None,
                    ex_provision=None, ex_provision_with=None, ex_no_up=False,
                    **kwargs):
        """
        @inherits: :class:`NodeDriver.create_node`

        :keyword name: The name for this new node (required)
        :type name: ``str``

        :keyword image: The image to use when creating this node (required)
        :type image: `NodeImage`

        :keyword size: The size of the node to create (required)
        :type size: `NodeSize`

        :keyword ex_vagrantfile: Vagrantfile location
                                 default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_box_url: box url. Takes precedence over `image.id`.
        :type ex_vagrantfile: ``str``

        :keyword ex_no_provision: if True, disable provisioning.  Same as 'ex_provision=False'.
        :type ex_no_provision: ``bool``

        :keyword ex_provider: Back the machine with a specific ex_provider
        :type ex_provider: ``str``

        :keyword ex_vm_name: name of VM.
        :type ex_vm_name: ``str``

        :keyword ex_provision_with: optional list of provisioners to enable.
        :type ex_provision_with: ``list`` of ``str``

        :keyword ex_provision: Enable or disable provisioning. Default behavior is to use the underlying vagrant default.
        :type ex_provision: ``bool``

        :keyword ex_no_up: Don't run `vagrant up`
        :type ex_no_up: ``bool``

        :return: Node that was created
        :rtype: ``Node`` or ``None``
        """

        ex_vagrantfile = ex_vagrantfile or path.join(getcwd(), 'Vagrantfile')
        vagrantfile_loc = Vagrantfiles._get_vagrantfile_dir(ex_vagrantfile)
        if not path.exists(vagrantfile_loc):
            mkdir(vagrantfile_loc)
        elif path.exists(ex_vagrantfile):
            raise OSError('`Vagrantfile` already exists at {!r}. '
                          'Remove it before running `vagrant init`/create_node.'.format(ex_vagrantfile))

        self.last_node_name = name

        self.vagrants.push(ex_vagrantfile)
        vagrant_obj = self.vagrants[ex_vagrantfile]

        subprocess.call([
            get_vagrant_executable(), 'init',
            ex_box_url if ex_box_url is not None else image.id if image is not None and image.id else '',
            '--output', ex_vagrantfile
        ])

        new_vagrantfile_kwargs = self.prepare_vagrantfile(name, size, ex_provider, kwargs)

        with open(ex_vagrantfile, 'r+') as f:
            new_vagrantfile = ''.join(
                VagrantfileEditor.parse_emit(blocks=new_vagrantfile_kwargs['blocks'],
                                             first_lines=new_vagrantfile_kwargs['first_lines'])(f))
            f.seek(0)
            f.write(new_vagrantfile)

        if size and size.disk is not None:
            log.warn('Not resizing disk, still implementation details TODO')  # TODO
            # self.resize_disk(ex_vm_name, name, new_vagrantfile_kwargs, size, vagrant_obj, first_run=True)

        if ex_no_up:
            try:
                status = vagrant_obj.status(vm_name=vagrant_obj.root)
            except subprocess.CalledProcessError as e:
                log.exception(e)
                status = None
                return Node(id=name, name=name,
                            state='poweroff? status check failed', driver=VagrantNodeDriver,
                            public_ips=[], private_ips=[],
                            extra=dict(ex_vagrantfile=path.join(vagrant_obj.root, 'Vagrantfile'),
                                       directory=vagrant_obj.root))
            return self.ex_parse_node(vagrantfile_obj=vagrant_obj,
                                      globalStatus=status,
                                      vm_name=ex_vm_name, node_name=name)
        else:
            return self.ex_start_node(vagrantfile=ex_vagrantfile,
                                      no_provision=ex_no_provision, provider=ex_provider,
                                      vm_name=ex_vm_name,
                                      node_name=name,
                                      provision=ex_provision, provision_with=ex_provision_with
                                      )

    @staticmethod
    def prepare_vagrantfile(name, size, ex_provider, kwargs):
        new_vagrantfile_kwargs = {}
        if size is not None:
            if size.ram:
                if size.extra is None:
                    size.extra = {'memory': size.ram}
                else:
                    size.extra['memory'] = size.ram
            if size.extra:
                new_vagrantfile_kwargs.update(**size.extra)
                new_vagrantfile_kwargs['provider'] = size.extra['provider']
        new_vagrantfile_kwargs['name'] = '"{}"'.format(name)

        first_lines = (
            'config.vm.hostname = "{}"'.format(name),
        )
        first_block = (
            {'name': 'config.vm.define "{name}"'.format(name=name), 'args': 'foo'},
            {'name': 'config.vm.provider', 'func_args': '"{}"'.format(ex_provider), 'args': 'v',
             'body_lines': ('v.name = "{}"'.format(name),)}
        )
        new_vagrantfile_kwargs['blocks'] = add_to(new_vagrantfile_kwargs['blocks'],
                                                  first_block,
                                                  new_vagrantfile_kwargs['blocks']
                                                  ) if 'blocks' in new_vagrantfile_kwargs else first_block
        # 'config.vm.provision "{name}", {d}'.format(name=name, d='{type: "shell", inline: "echo foo"}')
        new_vagrantfile_kwargs['first_lines'] = add_to(new_vagrantfile_kwargs['first_lines'], first_lines,
                                                       "new_vagrantfile_kwargs['first_lines']") \
            if 'first_lines' in new_vagrantfile_kwargs else first_lines
        if 'extras' in kwargs and kwargs['extras']:
            if 'first_lines' in kwargs['extras']:
                new_vagrantfile_kwargs['first_lines'] = add_to(new_vagrantfile_kwargs['first_lines'], '')  # \n
                new_vagrantfile_kwargs['first_lines'] = add_to(new_vagrantfile_kwargs['first_lines'],
                                                               kwargs['extras']['first_lines'],
                                                               "new_vagrantfile_kwargs['first_lines']")
            if 'blocks' in kwargs['extras']:
                new_vagrantfile_kwargs['blocks'] = add_to(kwargs['extras']['blocks'], new_vagrantfile_kwargs['blocks'],
                                                          "kwargs['extras']['blocks']")
        return new_vagrantfile_kwargs

    def resize_disk(self, ex_vm_name, name, new_vagrantfile_kwargs, size, vagrant_obj, first_run):
        if new_vagrantfile_kwargs['provider'] != '"virtualbox"':
            raise NotImplementedError('Disk manipulation only implemented for VirtualBox')

        if first_run:
            vagrant_obj.up(no_provision=True)
            vagrant_obj.halt(vm_name=ex_vm_name)

        vm_info = Virtualbox.get_dict(subprocess.check_output(['VBoxManage', 'showvminfo', name]))
        if not vm_info['State'].startswith('powered off'):
            raise OSError('Unable to manipulate HD with VM on. VM is {}'.format(vm_info['State']))

        hdd = next(hd for hd in imap(lambda hdd: Virtualbox.get_dict(
            subprocess.check_output(['VBoxManage', 'showhdinfo', hdd])),
                                     (v[:v.rfind('(') - 1] for v in vm_info.itervalues() if 'vmdk' in v or 'vdi' in v))
                   if hd['Size on disk'] != '0 MBytes')

        new_size = '{:d}'.format(size.disk * 1024)
        if hdd['Capacity'] == '{} MBytes'.format(new_size):
            return

        base = path.dirname(hdd['Location'])
        assert path.isdir(base)
        tmp_disk = 'tmp-disk.vdi'  # path.join(base,)
        rpl_disk = 'resized-disk.vmdk'  # path.join(base, )
        print('base =', base)
        print('tmp_disk =', tmp_disk)
        print('rpl_disk =', rpl_disk)
        call = partial(subprocess.call, stdout=stdout, stderr=stderr, cwd=base)
        call(['VBoxManage', 'clonehd', path.basename(hdd['Location']), tmp_disk, '--format', 'vdi'])
        call(['VBoxManage', 'internalcommands', 'sethduuid', tmp_disk])
        call(['VBoxManage', 'modifyhd', tmp_disk, '--resize', new_size])
        call(['VBoxManage', 'clonehd', tmp_disk, rpl_disk, '--format', 'vmdk'])
        call(['VBoxManage', 'internalcommands', 'sethduuid', rpl_disk])
        if path.isfile(path.join(base, tmp_disk)):
            remove(path.join(base, tmp_disk))
        else:
            print('Weird, not file:', path.join(base, tmp_disk))
        if path.isfile(hdd['Location']):
            remove(hdd['Location'])
        else:
            print('Weird, not file:', hdd['Location'])
        rename(path.join(base, rpl_disk), hdd['Location'])

    def reboot_node(self, ex_vagrantfile=None, ex_vm_name=None, ex_provision=None, ex_provision_with=None):
        """
        Reboot the given node

        @inherits :class:`NodeDriver.reboot_node`

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: name of VM.
                             default to ``None`` [default]
        :type ex_vm_name: ``str``

        :keyword ex_provision_with: optional list of provisioners to enable.
        :type ex_provision_with: ``list`` of ``str``

        :keyword ex_provision: Enable or disable provisioning. Default behavior is to use the underlying vagrant default.
        :type ex_provision: ``bool``
        """
        self.vagrants.push(ex_vagrantfile)
        self.vagrants[ex_vagrantfile].reload(ex_vm_name, ex_provision, ex_provision_with)

    def destroy_node(self, node, ex_vagrantfile=None, ex_vm_name=None):
        """
        @inherits :class:`NodeDriver.destroy_node`

        :keyword node: Not used
        :type node: ``any``

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: vm_name
                             default to ``None`` [default]
        :type ex_vm_name: ``str``
        """
        self.vagrants.push(ex_vagrantfile)
        self.vagrants[ex_vagrantfile].destroy(vm_name=ex_vm_name)

    def list_volume_snapshots(self, volume, ex_vagrantfile=None):
        """
        List snapshots for a storage volume.

        @inherites :class:`NodeDriver.list_volume_snapshots`

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        self.vagrants.push(ex_vagrantfile)
        return self.vagrants[ex_vagrantfile].snapshot_list()  # TODO parse a list of `VolumeSnapshot` from this

    def create_volume(self, size, name, location=None, snapshot=None, ex_vagrantfile=None):
        """
        Create a new volume.

        @inherites :class:`NodeDriver.create_volume`

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    def create_volume_snapshot(self, volume, name=None, ex_vagrantfile=None):
        """
        Creates a snapshot of the storage volume.

        @inherits :class:`NodeDriver.create_volume_snapshot`

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    def attach_volume(self, node, volume, device=None, ex_vagrantfile=None):
        """
        Attaches volume to node.

        @inherits :class:`NodeDriver.attach_volume`

        :keyword device: device path allocated for this attached volume
        :type device: ``str`` between /dev/xvdb to xvdz,
                      if empty, allocated by the system
        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    def detach_volume(self, volume, ex_instance_id=None, ex_vagrantfile=None):
        """
        Detaches a volume from a node.

        @inherits :class:`NodeDriver.detach_volume`

        :keyword ex_instance_id: the id of the instance from which the volume
                                 is detached.
        :type ex_instance_id: ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    def destroy_volume(self, volume, ex_vagrantfile=None):
        """
        Destroys a volume from a node.

        @inherits :class:`NodeDriver.destroy_volume`

        :keyword volume:
        :type volume: ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                      default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        """
        raise NotImplementedError()

    def destroy_volume_snapshot(self, snapshot, ex_vagrantfile=None):
        """
        Destroys a volume snapshot from a node.

        @inherits :class:`NodeDriver.destory_volume_snapshot`

        :keyword snapshot:
        :type snapshot: ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                      default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        """
        raise NotImplementedError()

    ##
    # Image management methods
    ##

    def list_images(self, location=None, ex_provider=None, ex_vagrantfile=None):
        """
        List images on a provider.

        @inherits :class:`NodeDriver.list_images`

        :keyword location: not used
        :type location: ``any``

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """

        if ex_provider:
            raise NotImplementedError('ex_provider is not yet implemented, TODO')

        self.vagrants.push(ex_vagrantfile)
        return [NodeImage(id=box.name, name=box.name,
                          driver=VagrantNodeDriver,
                          extra={'provider': box.provider,
                                 'version': box.version})
                for box in self.vagrants[ex_vagrantfile].box_list()]

    def create_image(self, node, name, description=None, ex_snapshot_id=None,
                     ex_image_version=None, ex_vagrantfile=None):
        """
        Creates an image from a system disk snapshot.

        @inherits :class:`NodeDriver.create_image`

        :keyword ex_snapshot_id: the id of the snapshot to create the image.
                                 (required)
        :type ex_snapshot_id: ``str``

        :keyword ex_image_version: the version number of the image
        :type ex_image_version: ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()
        # Maybe look at Packer?

    def delete_image(self, node_image, ex_vagrantfile=None):
        """
        @inherits :class:`NodeDriver.delete_image`

        :keyword node_image:

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    def get_image(self, image_id, ex_region_id=None, ex_vagrantfile=None):
        """
        @inherits :class:`NodeDriver.get_image`

        :keyword image_id:
        :keyword ex_region_id:

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :return: `NodeImage`
        """
        raise NotImplementedError()

    def copy_image(self, source_region, node_image, name, description=None,
                   ex_vagrantfile=None):
        """
        Copies an image from a source region to the destination region.
        If not provide a destination region, default to the current region.

        @inherits :class:`NodeDriver.copy_image`

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        raise NotImplementedError()

    @staticmethod
    def ex_parse_node(vagrantfile_obj, globalStatus, vm_name, node_name):
        """
        :keyword vagrantfile_obj: Vagrantfile obj
        :type vagrantfile_obj: ``Vagrant``

        :keyword globalStatus: globalStatus or Status obj
        :type globalStatus: ``GlobalStatus`` or ``Status``

        :keyword vm_name: vm_name
        :type vm_name: ``str``

        :keyword node_name: name of node (as wanted for etcd, hostname &etc).
        :type node_name: ``str``

        :return Node that was parsed out
        :rtype ``Node``
        """
        if globalStatus.state.startswith('poweroff') or globalStatus.state == 'not_created':
            return Node(id=globalStatus.id, name=node_name,
                        state=globalStatus.state, driver=VagrantNodeDriver,
                        public_ips=[], private_ips=[],
                        extra=dict(provider=globalStatus.provider,
                                   ex_vagrantfile=path.join(vagrantfile_obj.root, 'Vagrantfile'),
                                   directory=vagrantfile_obj.root))

        ssh_config = vagrantfile_obj.conf(vm_name=vm_name)
        hostname = vagrantfile_obj.hostname(vm_name=vm_name)

        if isIpPrivate(hostname):
            private_ips = [hostname]
            public_ips = [hostname]  # Hmm, I think for Vagrant we should set this?
        else:
            private_ips = []
            public_ips = [hostname]
        extra = {'user': vagrantfile_obj.user(vm_name=globalStatus.id) if ssh_config is not None else ssh_config,
                 'ssh_config': ssh_config,
                 'user_hostname_port': vagrantfile_obj.user_hostname_port(vm_name=globalStatus.id)}

        return Node(id=globalStatus.id, name=node_name,
                    state=globalStatus.state, driver=VagrantNodeDriver,
                    public_ips=public_ips, private_ips=private_ips,
                    extra=dict(provider=globalStatus.provider,
                               ex_vagrantfile=path.join(globalStatus.directory, 'Vagrantfile'),
                               directory=globalStatus.directory, **extra))

    def ex_start_node(self, vagrantfile, no_provision=False, provider=None, vm_name=None,
                      node_name=None, provision=None, provision_with=None):
        """
        Start node to running state.

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :keyword node: the ``Node`` object to start
        :type node: ``Node``

        :keyword no_provision: if True, disable provisioning.  Same as 'provision=False'.
        :type no_provision: ``bool``

        :keyword provider: Back the machine with a specific provider
        :type provider: ``str``

        :keyword vm_name: name of VM.
        :type vm_name: ``str``

        :keyword provision_with: optional list of provisioners to enable.
        :type provision_with: ``list`` of ``str``

        :keyword node_name: name of node (as wanted for etcd, hostname &etc).
        :type node_name: ``str``

        :keyword provision: Enable or disable provisioning. Default behavior is to use the underlying vagrant default.
        :type provision: ``bool``

        :return: Node that was created
        :rtype: ``Node``
        """
        self.vagrants.push(vagrantfile)
        status = self.vagrants[vagrantfile].up(no_provision=no_provision, provider=provider, vm_name=vm_name,
                                               provision=provision, provision_with=provision_with)
        vm_name = vm_name or status.vm_name
        if vm_name is None or vm_name.isdigit():
            # Status gets some weird other ID `machine.name[:max_name_length]`
            return next(node for node in self.list_nodes()
                        if node.extra['ex_vagrantfile'] == vagrantfile)

        return self.ex_parse_node(vagrantfile_obj=self.vagrants[vagrantfile],
                                  globalStatus=status,
                                  vm_name=vm_name, node_name=node_name)

    def ex_stop_node(self, vagrantfile, vm_name=None, ex_force_stop=False):
        """
        Stop a running node.

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :keyword ex_force_stop: if ``True``, stop node force (maybe lose data)
                                otherwise, stop node normally,
                                default to ``False``
        :type ex_force_stop: ``bool``
        """
        self.vagrants.push(vagrantfile)
        self.vagrants[vagrantfile].halt(vm_name, ex_force_stop)

    def ex_get_conn_info(self, vagrantfile, vm_name=None, ex_force_stop=False):
        """
        Get connection information. Usually provided in the Node object.

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :keyword ex_force_stop: if ``True``, stop node force (maybe lose data)
                                otherwise, stop node normally,
                                default to ``False``
        :type ex_force_stop: ``bool``

        :return: stopping operation result.
        :rtype: ``{ user: string, ssh_config: {} }``
        """
        self.vagrants.push(vagrantfile)
        vagrant = self.vagrants[vagrantfile]
        return {'user': vagrant.user(), 'ssh_config': vagrant.conf()}

    def ex_parse_vagrantfile(self, ex_vagrantfile, method=None):
        """
        :keyword ex_vagrantfile: Vagrantfile location
                                 default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword method: Method to call on the parsed otuput;
                 e.g.: 'configure_version', 'serializable', 'to_dict', 'to_dict_iter', 'to_dict_obj', 'vm'
        :type ex_vagrantfile: ``str``

        :return: parsed Vagrantfile object
        :rtype: ``Vagrantfile``
        """
        ex_vagrantfile = ex_vagrantfile or path.join(getcwd(), 'Vagrantfile')
        vagrantfile_loc = Vagrantfiles._get_vagrantfile_dir(ex_vagrantfile)

        with open(ex_vagrantfile) as f:
            parsed = VagrantParser.parses(content=f.read())
        return parsed if method is None else getattr(parsed, method)()


class Vagrantfiles(object):
    vagrantfiles = {}

    def __init__(self, vagrantfiles):
        """
        Instantiate Vagrantfiles object

        :keyword vagrantfiles: Vagrantfile locations
        :type ex_vagrantfile: ``list`` of ``str``
        """
        self.vagrantfiles = dict(imap(lambda vagrantfile: (vagrantfile, self.push(vagrantfile)), vagrantfiles)
                                 ) if vagrantfiles else {}

    def __getitem__(self, vagrantfile):
        """
        Return Vagrant object at vagrantfile location

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :return: Vagrant object
        :rtype: ``Vagrant``
        """
        return self.vagrantfiles[self._get_vagrantfile_dir(vagrantfile or getcwd())]

    def __setitem__(self, vagrantfile, vagrant_obj):
        """
        Sets Vagrant object

        :keyword vagrantfile: Vagrantfile location
        :type vagrantfile: ``str``

        :keyword vagrant_obj: Vagrant object
        :type vagrant_obj: ``Vagrant``
        """
        self.vagrantfiles.update({vagrantfile: vagrant_obj})

    def __delitem__(self, vagrantfile):
        """
        Delete Vagrant object stored in memory that matches arg

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``
        """
        del self.vagrantfiles[self._get_vagrantfile_dir(vagrantfile or getcwd())]

    def push(self, vagrantfile=None, quiet_stdout=True, quiet_stderr=True,
             env=None, out_cm=None, err_cm=None):
        """
        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :keyword quiet_stdout: Ignored if out_cm is not None.  If True, the stdout of
          vagrant commands whose output is not captured for further processing
          will be sent to devnull.
        :type quiet_stdout: ``bool``

        :keyword quiet_stderr: Ignored if out_cm is not None.  If True, the stderr of
          vagrant commands whose output is not captured for further processing
          will be sent to devnull.
        :type quiet_stderr: ``bool``

        :keyword env: a dict of environment variables (string keys and values) passed to
          the vagrant command subprocess or None.  Defaults to None.  If env is
          None, `subprocess.Popen` uses the current process environment.
        :type env: ``dict``

        :keyword out_cm: a no-argument function that returns a ContextManager that
          yields a filehandle or other object suitable to be passed as the
          `stdout` parameter of a subprocess that runs a vagrant command.
          Using a context manager allows one to close the filehandle in case of
          an Exception, if necessary.  Defaults to none_cm, a context manager
          that yields None.  See `make_file_cm` for an example of
          how to log stdout to a file.  Note that commands that parse the
          output of a vagrant command, like `status`, capture output for their
          own use, ignoring the value of `out_cm` and `quiet_stdout`.
        :type out_cm: ``() -> ContextManager -> File``

        :keyword err_cm: a no-argument function that returns a ContextManager, like
          out_cm, for handling the stderr of the vagrant subprocess.  Defaults
          to none_cm.
        :type err_cm: ``() -> ContextManager -> File``
        """
        vagrant_dir = self._get_vagrantfile_dir(vagrantfile or getcwd())
        self[vagrant_dir] = Vagrant(root=vagrant_dir, quiet_stdout=quiet_stdout,
                                    quiet_stderr=quiet_stderr,
                                    env=env, out_cm=out_cm, err_cm=err_cm)

    def clear(self):
        """
        Delete all Vagrant objects stored in memory
        """
        self.vagrantfiles.clear()

    @staticmethod
    def _get_vagrantfile_dir(vagrantfile):
        """
        Given a ``vagrantfile`` argument, resolves its directory (or itself if it is a directory).

        :keyword vagrantfile: Vagrantfile location
                              If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``
        """
        if vagrantfile is None:
            return
        return path.dirname(vagrantfile) if path.isfile(vagrantfile) or vagrantfile.endswith(
            '{}Vagrantfile'.format(path.sep)) else vagrantfile
