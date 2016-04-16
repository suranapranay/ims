#! /usr/bin/python

import unittest
import os
from operations import *
import uuid
import time
class Tests(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass

## ## ## we use UID's for unique string generation. 
## 4 byte string.This works for Ceph.



## Note : we are using os.system("rbd ...") because the snapshot
## functions in rbd seem to have a bug. The image goes into a 
## permanent busy loop.
## this is bad.

## there is no point in testing just the snapshot creationg funciton
## This is because we test it with remove and list. 
## The list test is the one that reliably tests
## Both list and create_snap.
    def test_provision(self):    
        rnd_str = str(uuid.uuid4())
        print rnd_str
        prov = provision(rnd_str)
        print prov
        if prov['retval'] == True:
            img_list = list_all_images(True)["retval"]
            self.assertTrue(rnd_str in img_list)
            detach_node(rnd_str) 

    def test_removal(self):    
        try:
            rnd_str = str(uuid.uuid4())
            print rnd_str
            if provision(rnd_str) == ret_200(True):
                img_list = list_all_images(True)["retval"]
                self.assertTrue(rnd_str in img_list)
                detach_node(rnd_str) 
                img_list_afterdetach = list_all_images(True)["retval"]
                self.assertFalse(rnd_str in img_list_afterdetach)
            else:
                self.assertFalse(True) 
        except:
            pass

    def test_snapshot_list(self):
        rnd_str = str(uuid.uuid4())
        rnd_str2 = str(uuid.uuid4())
        if provision(rnd_str) == ret_200(True):
            self.assertTrue(create_snapshot(rnd_str, rnd_str2) == ret_200(True))
            self.assertTrue(rnd_str2 in list_snaps(rnd_str)['retval']) # the actual testcase, other things are tested.
            os.system("rbd snap purge {0}".format(rnd_str))
            if detach_node(rnd_str) == ret_200(True):
                self.assertTrue(True)
            else:
                self.assertFalse(True)

        else:
            self.assertTrue(False)

    def test_snapshot_remove(self):
        rnd_str = str(uuid.uuid4())
        rnd_str2 = str(uuid.uuid4())
        if provision(rnd_str) == ret_200(True):
            self.assertTrue(create_snapshot(rnd_str, rnd_str2) == ret_200(True))
            self.assertTrue(rnd_str2 in list_snaps(rnd_str)['retval']) #validate that the snapshot is created.
            self.assertTrue(remove_snaps(rnd_str, rnd_str2)['retval'])
            self.assertTrue(rnd_str2 not in list_snaps(rnd_str)['retval'])
            if detach_node(rnd_str) == ret_200(True):
                self.assertTrue(True)
            else:
                 self.assertFalse(True)
        else:
            self.assertTrue(False)

      

if __name__ == "__main__":
    unittest.main()                 
