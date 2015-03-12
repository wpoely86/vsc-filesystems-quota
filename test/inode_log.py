#!/usr/bin/env python
#
# Copyright 2015-2015 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
"""
Tests for the inode_log.py script in vsc-filesystem-quota.

@author: Andy Georges
"""

from unittest import TestCase, TestLoader, TestSuite

from vsc.filesystem.gpfs import GpfsQuota
from vsc.filesystem.quota.process import InodeCritical, process_inodes_information


class TestMailAdmin(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


class TestProcessInodesInformation(TestCase):
    """
    Test the function that processes the information regarding inode usage to determine
    for which filesets the usage has become critical, i.e.m exceeded the given threshold.
    """

    def setUp(self):
        """
        Add example cases.
        """

        # names reflect the number of used inodes (max is set by default at 100, so this effectively is a percentage.
        self.names = (10, 95)

        self.filesets = {
            self.names[0]: {
                'allocInodes': 90,
                'filesetName': '%d' % self.names[0],
                'maxInodes': 100,
            },
            self.names[1]: {
                'allocInodes': 90,
                'filesetName': '%d' % self.names[1],
                'maxInodes': 100,
            }
        }

        self.defaultQuota = GpfsQuota(
            name="testQuota",
            blockUsage=0,
            blockQuota=0,
            blockLimit=0,
            blockInDoubt=0,
            blockGrace=0,
            filesUsage=0,
            filesQuota=0,
            filesLimit=0,
            filesInDoubt=0,
            filesGrace=0,
            remarks="",
            quota=0,
            defQuota=0,
            fid=0,
            filesetname="default"
        )

        self.usage = {
           self.names[0]: [self.defaultQuota._replace(filesUsage=self.names[0])],
           self.names[1]: [self.defaultQuota._replace(filesUsage=self.names[1])],
        }

    def testThreshold(self):
        """
        Verify that only entries that have a percentage above the threshold are marked as critical.
        """
        critical = process_inodes_information(self.filesets, self.usage, threshold=0.9)

        self.assertDictEqual(
            critical,
            { str(self.names[1]): InodeCritical(used=self.usage[self.names[1]][0].filesUsage,
                                                allocated=self.filesets[self.names[1]]['allocInodes'],
                                                maxinodes=self.filesets[self.names[1]]['maxInodes']),
            },
            "computed dict with critical filesets is the expected dict"
        )



def suite():
    """ return all the tests"""
    s = TestSuite()
    s.addTests(TestLoader().loadTestsFromTestCase(TestMailAdmin))
    s.addTests(TestLoader().loadTestsFromTestCase(TestProcessInodesInformation))
