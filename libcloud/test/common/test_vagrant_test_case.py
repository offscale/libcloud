"""
Tests for the various functionality provided by the VagrantTestCase class

There are a handful of classes to try to provide multiple different varying samples of possible setups
"""
import os
from unittest import TestCase
from unittest.util import safe_repr

from libcloud.common.vagrant import Vagrant, stderr_cm


def get_vagrant_root(test_vagrant_root_path):
    return os.path.dirname(os.path.realpath(__file__)) + '/vagrantfiles/' + test_vagrant_root_path


SINGLE_BOX = get_vagrant_root('single_box')
MULTI_BOX = get_vagrant_root('multi_box')


class VagrantTestCase(TestCase):
    """
    TestCase class to control vagrant boxes during testing

    vagrant_boxes: An iterable of vagrant boxes. If empty or None, all boxes will be used. Defaults to []
    vagrant_root: The root directory that holds a Vagrantfile for configuration. Defaults to the working directory
    restart_boxes: If True, the boxes will be restored to their initial states between each test, otherwise the boxes
        will remain up. Defaults to False
    """

    vagrant_boxes = []
    vagrant_root = None
    restart_boxes = False

    __initial_box_statuses = {}
    __cleanup_actions = {
        Vagrant.NOT_CREATED: 'destroy',
        Vagrant.POWEROFF: 'halt',
        Vagrant.SAVED: 'suspend',
    }

    def __init__(self, *args, **kwargs):
        """Check that the vagrant_boxes attribute is not left empty, and is populated by all boxes if left blank"""
        self.vagrant = Vagrant(self.vagrant_root, err_cm=stderr_cm)
        if not self.vagrant_boxes:
            boxes = [s.name for s in self.vagrant.status()]
            if len(boxes) == 1:
                self.vagrant_boxes = ['default']
            else:
                self.vagrant_boxes = boxes
        super(VagrantTestCase, self).__init__(*args, **kwargs)

    def assertBoxStatus(self, box, status):
        """Assertion for a box status"""
        box_status = [s.state for s in self.vagrant.status() if s.name == box][0]
        if box_status != status:
            self.failureException('{} has status {}, not {}'.format(box, box_status, status))

    def assertBoxUp(self, box):
        """Assertion for a box being up"""
        self.assertBoxStatus(box, Vagrant.RUNNING)

    def assertBoxSuspended(self, box):
        """Assertion for a box being up"""
        self.assertBoxStatus(box, Vagrant.SAVED)

    def assertBoxHalted(self, box):
        """Assertion for a box being up"""
        self.assertBoxStatus(box, Vagrant.POWEROFF)

    def assertBoxNotCreated(self, box):
        """Assertion for a box being up"""
        self.assertBoxStatus(box, Vagrant.NOT_CREATED)

    def run(self, result=None):
        """Override run to have provide a hook into an alternative to tearDownClass with a reference to self"""
        self.setUpOnce()
        run = super(VagrantTestCase, self).run(result)
        self.tearDownOnce()
        return run

    def setUpOnce(self):
        """Collect the box states before starting"""
        for box_name in self.vagrant_boxes:
            box_state = [s.state for s in self.vagrant.status() if s.name == box_name][0]
            self.__initial_box_statuses[box_name] = box_state

    def tearDownOnce(self):
        """Restore all boxes to their initial states after running all tests, unless tearDown handled it already"""
        if not self.restart_boxes:
            self.restore_box_states()

    def restore_box_states(self):
        """Restores all boxes to their original states"""
        for box_name in self.vagrant_boxes:
            action = self.__cleanup_actions.get(self.__initial_box_statuses[box_name])
            if action:
                getattr(self.vagrant, action)(vm_name=box_name)

    def setUp(self):
        """Starts all boxes before running tests"""
        for box_name in self.vagrant_boxes:
            self.vagrant.up(vm_name=box_name)

        super(VagrantTestCase, self).setUp()

    def tearDown(self):
        """Returns boxes to their initial status after each test if self.restart_boxes is True"""
        if self.restart_boxes:
            self.restore_box_states()

        super(VagrantTestCase, self).tearDown()

    # You're welcome Python =< 2.6
    def assertGreater(self, a, b, msg=None):
        """Just like self.assertTrue(a > b), but with a nicer default message."""
        if not a > b:
            standardMsg = '%s not greater than %s' % (safe_repr(a), safe_repr(b))
            self.fail(self._formatMessage(msg, standardMsg))

class AllMultiBoxesTests(VagrantTestCase):
    """Tests for a multiple box setup where vagrant_boxes is left empty"""

    vagrant_root = MULTI_BOX

    def test_default_boxes_list(self):
        """Tests that all boxes in a Vagrantfile if vagrant_boxes is not defined"""
        self.assertGreater(len(self.vagrant_boxes), 0)


class SingleBoxTests(VagrantTestCase):
    """Tests for a single box setup"""

    vagrant_root = SINGLE_BOX

    def test_box_up(self):
        """Tests that the box starts as expected"""
        state = self.vagrant.status(vm_name=self.vagrant_boxes[0])[0].state
        self.assertEqual(state, Vagrant.RUNNING)


class SpecificMultiBoxTests(VagrantTestCase):
    """Tests for a multiple box setup where only some of the boxes are to be on"""

    vagrant_boxes = ['precise32']
    vagrant_root = MULTI_BOX

    def test_all_boxes_up(self):
        """Tests that all boxes listed are up after starting"""
        for box_name in self.vagrant_boxes:
            state = self.vagrant.status(vm_name=box_name)[0].state
            self.assertEqual(state, Vagrant.RUNNING)

    def test_unlisted_boxes_ignored(self):
        """Tests that the boxes not listed are not brought up"""
        for box_name in [s.name for s in self.vagrant.status()]:
            if box_name in self.vagrant_boxes:
                self.assertBoxUp(box_name)
            else:
                self.assertBoxNotCreated(box_name)
