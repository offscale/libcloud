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

from os import path

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.compute.base import Node, NodeDriver, NodeImage, NodeSize
from libcloud.compute.types import NodeState, StorageVolumeState, VolumeSnapshotState

from vagrant import compat, Vagrant


class VagrantDriver(NodeDriver):
    """
    Vagrant node driver.
    """

    name = 'Vagrant'
    website = 'https://www.vagrantup.com'
    features = {'create_node': ['password']}

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

    _vagrants = {}

    def __init__(self, *args, **kwargs):
        super(VagrantDriver, self).__init__(*args, **kwargs)

    def list_nodes(self, ex_vagrantfile=None, ex_provider=None):
        """
        List all nodes.

        @inherits: :class:`NodeDriver.create_node`

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``

        :keyword ex_vagrantfile: Vagrantfile location
                                 default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :return: list of Node
        :rtype: ``[Node]``
        """
        self.ex_add_vagrantfile(ex_vagrantfile)

        # TODO: parse this result into a list of Node
        print('{!r}'.format(
            self.ex_get_vagrant(ex_vagrantfile)._run_vagrant_command(['global-status', '--machine-readable'])))

    def list_sizes(self, location=None):
        raise NotImplementedError('N/A for Vagrant')

    def list_locations(self):
        raise NotImplementedError('N/A for Vagrant')

    def create_node(self, name, size, image, auth=None, ex_vagrantfile=None, **kwargs):
        """
        @inherits: :class:`NodeDriver.create_node`

        :param name: The name for this new node (required)
        :type name: ``str``

        :param image: The image to use when creating this node (required)
        :type image: `NodeImage`

        :param size: The size of the node to create (required)
        :type size: `NodeSize`

        :keyword ex_vagrantfile: Vagrantfile location
                                 default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """

        raise NotImplementedError()

    def reboot_node(self, ex_vagrantfile=None, ex_vm_name=None):
        """
        Reboot the given node

        @inherits :class:`NodeDriver.reboot_node`

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: vm_name
                             default to ``None`` [default]
        :type ex_vm_name: ``str``
        """
        return self.ex_get_vagrant(ex_vagrantfile).destroy(ex_vm_name)

    def destroy_node(self, node, ex_vagrantfile=None, ex_vm_name=None):
        """
        :keyword node: Not used
        :type node: ``any``

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``

        :keyword ex_vm_name: vm_name
                             default to ``None`` [default]
        :type ex_vm_name: ``str``
        """
        return self.ex_get_vagrant(ex_vagrantfile).destroy(vm_name=ex_vm_name)

    def list_volume_snapshots(self, volume, ex_vagrantfile=None):
        """
        List snapshots for a storage volume.

        @inherites :class:`NodeDriver.list_volume_snapshots`

        :keyword ex_vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type ex_vagrantfile: ``str``
        """
        return self.ex_get_vagrant(ex_vagrantfile).snapshot_list()

    def create_volume(self, size, name, location=None, snapshot=None):
        """
        Create a new volume.

        @inherites :class:`NodeDriver.create_volume`
        """
        raise NotImplementedError()

    def create_volume_snapshot(self, volume, name=None, ex_description=None,
                               ex_client_token=None):
        """
        Creates a snapshot of the storage volume.

        @inherits :class:`NodeDriver.create_volume_snapshot`

        :keyword ex_description: description of the snapshot.
        :type ex_description: ``unicode``

        :keyword ex_client_token: a token generated by client to identify
                                  each request.
        :type ex_client_token: ``str``
        """
        raise NotImplementedError()

    def attach_volume(self, node, volume, device=None,
                      ex_delete_with_instance=None):
        """
        Attaches volume to node.

        @inherits :class:`NodeDriver.attach_volume`

        :keyword device: device path allocated for this attached volume
        :type device: ``str`` between /dev/xvdb to xvdz,
                      if empty, allocated by the system
        :keyword ex_delete_with_instance: if to delete this volume when the
                                          instance is deleted.
        :type ex_delete_with_instance: ``bool``
        """
        raise NotImplementedError()

    def detach_volume(self, volume, ex_instance_id=None):
        """
        Detaches a volume from a node.

        @inherits :class:`NodeDriver.detach_volume`

        :keyword ex_instance_id: the id of the instance from which the volume
                                 is detached.
        :type ex_instance_id: ``str``
        """
        raise NotImplementedError()

    def destroy_volume(self, volume):
        raise NotImplementedError()

    def destroy_volume_snapshot(self, snapshot):
        raise NotImplementedError()

    ##
    # Image management methods
    ##

    def list_images(self, location=None, ex_provider=None):
        """
        List images on a provider.

        @inherits :class:`NodeDriver.list_images`

        :keyword location: not used
        :type location: ``any``

        :keyword ex_provider: a list of providers to filter the images returned. Defaults to all.
        :type ex_provider: ``list`` of ``str``
        """

        if ex_provider:
            raise NotImplementedError('ex_provider is not yet implemented, TODO')

        return [line[line.rfind(',') + 1:] for line in compat.decode(
            subprocess.check_output('vagrant box list --machine-readable', shell=True)).splitlines()
                if 'box-name' in line]

    def create_image(self, node, name, description=None, ex_snapshot_id=None,
                     ex_image_version=None, ex_client_token=None):
        """
        Creates an image from a system disk snapshot.

        @inherits :class:`NodeDriver.create_image`

        :keyword ex_snapshot_id: the id of the snapshot to create the image.
                                 (required)
        :type ex_snapshot_id: ``str``

        :keyword ex_image_version: the version number of the image
        :type ex_image_version: ``str``

        :keyword ex_client_token: a token generated by client to identify
                                  each request.
        :type ex_client_token: ``str``
        """
        raise NotImplementedError()

    def delete_image(self, node_image):
        raise NotImplementedError()

    def get_image(self, image_id, ex_region_id=None):
        raise NotImplementedError()

    def copy_image(self, source_region, node_image, name, description=None,
                   ex_destination_region_id=None, ex_client_token=None):
        """
        Copies an image from a source region to the destination region.
        If not provide a destination region, default to the current region.

        @inherits :class:`NodeDriver.copy_image`

        :keyword ex_destination_region_id: id of the destination region
        :type ex_destination_region_id: ``str``

        :keyword ex_client_token: a token generated by client to identify
                                  each request.
        :type ex_client_token: ``str``
        """
        raise NotImplementedError()

    def ex_add_vagrantfile(self, vagrantfile=None, quiet_stdout=True, quiet_stderr=True,
                           env=None, out_cm=None, err_cm=None):
        """
        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :param quiet_stdout: Ignored if out_cm is not None.  If True, the stdout of
          vagrant commands whose output is not captured for further processing
          will be sent to devnull.
        :type quiet_stdout: ``bool``

        :param quiet_stderr: Ignored if out_cm is not None.  If True, the stderr of
          vagrant commands whose output is not captured for further processing
          will be sent to devnull.
        :type quiet_stderr: ``bool``

        :param env: a dict of environment variables (string keys and values) passed to
          the vagrant command subprocess or None.  Defaults to None.  If env is
          None, `subprocess.Popen` uses the current process environment.
        :type env: ``dict``

        :param out_cm: a no-argument function that returns a ContextManager that
          yields a filehandle or other object suitable to be passed as the
          `stdout` parameter of a subprocess that runs a vagrant command.
          Using a context manager allows one to close the filehandle in case of
          an Exception, if necessary.  Defaults to none_cm, a context manager
          that yields None.  See `make_file_cm` for an example of
          how to log stdout to a file.  Note that commands that parse the
          output of a vagrant command, like `status`, capture output for their
          own use, ignoring the value of `out_cm` and `quiet_stdout`.
        :type out_cm: ``() -> ContextManager -> File``

        :param err_cm: a no-argument function that returns a ContextManager, like
          out_cm, for handling the stderr of the vagrant subprocess.  Defaults
          to none_cm.
        :type err_cm: ``str``
        """
        vagrant_dir = self._get_vagrantfile_dir(vagrantfile)
        self._vagrants.update({vagrant_dir: Vagrant(root=vagrant_dir, quiet_stdout=quiet_stdout,
                                                    quiet_stderr=quiet_stderr,
                                                    env=env, out_cm=out_cm, err_cm=err_cm)})

    def ex_del_vagrantfile(self, vagrantfile=None):
        """
        Delete Vagrant object stored in memory that matches arg

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``
        """
        del self._vagrants[self._get_vagrantfile_dir(vagrantfile)]

    def ex_clear_vagrantfiles(self):
        """
        Delete all Vagrant objects stored in memory
        """
        self._vagrants.clear()

    def ex_get_vagrant(self, vagrantfile=None):
        """
        Return Vagrant object of root

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :return: Vagrant object
        :rtype: ``vagrant.Vagrant``
        """
        return self._vagrants[self._get_vagrantfile_dir(vagrantfile)]

    def ex_start_node(self, vagrantfile, no_provision=False, provider=None, vm_name=None,
                      provision=None, provision_with=None):
        """
        Start node to running state.

        :keyword vagrantfile: Vagrantfile location
                              default to ``None`` [this dir]. If a filepath is given, its dir is resolved.
        :type vagrantfile: ``str``

        :param node: the ``Node`` object to start
        :type node: ``Node``

        :param no_provision: if True, disable provisioning.  Same as 'provision=False'.
        :type no_provision: ``bool``

        :param provider: Back the machine with a specific provider
        :type provider: ``str``

        :param vm_name: name of VM.
        :type vm_name: ``str``

        :param provision_with: optional list of provisioners to enable.
        :type provision_with: ``list`` of ``str``

        :param provision: Enable or disable provisioning. Default behavior is to use the underlying vagrant default.
        :type provision: ``bool``

        :return: starting operation result.
        :rtype: ``bool``
        """
        return self.ex_get_vagrant(vagrantfile).up(no_provision, provider, vm_name, provision, provision_with)

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

        :return: stopping operation result.
        :rtype: ``bool``
        """
        return self.ex_get_vagrant(vagrantfile).halt(vm_name, ex_force_stop)

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
        :rtype: ``bool``
        """
        return {'user': self.ex_get_vagrant(vagrantfile).user(),
                'ssh_config': self.ex_get_vagrant(vagrantfile).ssh_config()}

    @staticmethod
    def _get_vagrantfile_dir(vagrantfile):
        if vagrantfile is None:
            return vagrantfile
        return path.dirname(vagrantfile) if path.isfile(vagrantfile) else vagrantfile
