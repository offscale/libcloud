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
import subprocess
from itertools import imap
from os import path, getcwd

from contrib.utils import isIpPrivate
from libcloud.common.types import LibcloudError

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

from vagrant import compat, Vagrant, get_vagrant_executable
from vagrant2json import vagrant2dict


class VagrantDriver(NodeDriver):
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
        :type ex_vagrantfile: ``list`` of ``str``
        """
        super(VagrantDriver, self).__init__(*args, **kwargs)
        self.vagrants = Vagrantfiles([ex_vagrantfile] if isinstance(ex_vagrantfile, basestring) else ex_vagrantfile)

    def list_nodes(self, ex_vagrantfile=None, ex_vm_name=None, ex_provider=None):
        """
        List all nodes.

        @inherits: :class:`NodeDriver.create_node`

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                     default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: required in a multi-VM environment.
        :type ex_vm_name: ``str``

        :return: list of Node
        :rtype: ``list`` of ``Node``
        """
        self.vagrants.push(ex_vagrantfile)
        vagrant = self.vagrants[ex_vagrantfile]

        try:
            ssh_config = vagrant.conf(ex_vm_name)
        except subprocess.CalledProcessError as e:
            raise LibcloudError(value='[{e.cmd} {e.args}] $?={e.returncode} {e.output}: {e.message}'.format(e=e),
                                driver=VagrantDriver)

        if isIpPrivate(ssh_config['HostName']):
            public_ips = [ssh_config['HostName'] + ssh_config['Port']]
            private_ips = []
        else:
            public_ips = []
            private_ips = [ssh_config['HostName'] + ssh_config['Port']]

        tuple_of_status_dicts = vagrant2dict(
            StringIO(vagrant._run_vagrant_command(['global-status', '--machine-readable']))
        )

        return [Node(id=status_dict['id'], name=status_dict['name'],
                     state=status_dict['state'], driver=VagrantDriver,
                     public_ips=public_ips, private_ips=private_ips,
                     extra={'provider': status_dict['provider'],
                            'directory': status_dict['directory'],
                            'user': vagrant.user(ex_vm_name),
                            'ssh_config': ssh_config
                            })
                for status_dict in tuple_of_status_dicts]

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
                    ex_no_provision=False, ex_provider=None, ex_vm_name=None,
                    ex_provision=None, ex_provision_with=None,
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

        :return: starting operation result.
        :rtype: ``bool``
        """

        if size is not None:
            raise NotImplementedError('size for create_node')
        vagrantfile_loc = Vagrantfiles._get_vagrantfile_dir(ex_vagrantfile or getcwd())
        subprocess.call([
            get_vagrant_executable(), 'init', '{name} {box_url}'.format(
                box_url=ex_box_url if ex_box_url is not None else image.id if image is not None and image.id else '',
                name=path.basename(ex_vagrantfile)),
            '--output "{vagrantfile_loc}"'.format(vagrantfile_loc=vagrantfile_loc)
        ])
        return self.ex_start_node(vagrantfile=vagrantfile_loc,
                                  no_provision=ex_no_provision, provider=ex_provider,
                                  vm_name=ex_vm_name, provision=ex_provision, provision_with=ex_provision_with
                                  )

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
                          driver=VagrantDriver,
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

    def ex_start_node(self, vagrantfile, no_provision=False, provider=None, vm_name=None,
                      provision=None, provision_with=None):
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

        :keyword provision: Enable or disable provisioning. Default behavior is to use the underlying vagrant default.
        :type provision: ``bool``
        """
        self.vagrants.push(vagrantfile)
        self.vagrants[vagrantfile].up(no_provision, provider, vm_name, provision, provision_with)

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
        :rtype: ``vagrant.Vagrant``
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
        :type err_cm: ``str``
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
        return path.dirname(vagrantfile) if path.isfile(vagrantfile) else vagrantfile
