#!/usr/bin/python
import sys
from ceph_wrapper import *
import ConfigParser, os
import subprocess
import os.path
import urlparse
import requests
import json
import time
#As of now a shell script that can accomodate rbd map and update.
#Since there are no python rbd map and updates wrappers around ceph.
class ShellScriptException(Exception): 
    def __init__(self, message = None, errors = None):
        super(ShellScriptException, self).__init__(message)
        if not errors:
            errors = {"status_code" : 999} 
        self.errors = errors   

class HaasException(Exception): 
    def __init__(self, message = None, errors = None):
        super(HaasException, self).__init__(message)
        print errors
        self.errors = errors

class GlobalConfig(object):
    # once we have a config file going, this object will parse the config file.
    # for the time being we are going to hard code the inits.
        
    def __init__(self):
        try:
            self.configfile = 'bmiconfig.cfg'
        except Exception, e:
            print e
    #add parser code here once we have a configfile/ if we decide on a Db
    # we put the db code here.
    def __str__(self):
        return {'file system :' : self.fstype, \
            'rid' : self.rid, 'pool' : self.pool,\
                'configfile' : self.r_conf} 

    def parse_config(self):
        config = ConfigParser.SafeConfigParser()
        try:
            config.read(self.configfile)
            for k, v in config.items('filesystem'):
                if v == 'True':
                    self.fstype = k
            if self.fstype == 'ceph':
                self.rid = config.get(self.fstype, 'id')
                self.pool = config.get(self.fstype, 'pool')
                self.r_conf = config.get(self.fstype, 'conf_file')
                self.keyring = config.get(self.fstype, 'keyring')
        except ConfigParser.NoOptionError, err: #which is same as 'exp as e'
            print str(err)

#Calling shell script which executes a iscsi update as we don't have 
#rbd map in documentation.
def call_shellscript(path, m_args):
        arglist = []
        arglist.append(path)
        for arg in m_args:
                arglist.append(arg)
        proc = subprocess.Popen(arglist, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return proc.communicate()

#Custom Exception Class that we are going to use which is a wrapper around
#Ceph exception classes.
class ResponseException(object):
    def __init__(self, e, debug = False):
        self.exception_dict = dict()
        self.exception = e
        if debug:
            print self.exception
# Extend this dict as needed for future expceptions. this
# ensures readeability and uniformity of errors.
        self.exception_dict = {
           type(rbd.ImageExists())  : 471,
           type(CephException()) : 472,
           type(rbd.ImageBusy()) : 473,
           type(rbd.ImageHasSnapshots()) : 474,
           type(rbd.ImageNotFound()) : 475,
           type(rbd.FunctionNotSupported()) : 476,
           type(rbd.ArgumentOutOfRange()) : 477,
           type(ShellScriptException()) : 478,
           type(requests.exceptions.ConnectionError()) : 441,
           type(HaasException()) : self.exception.errors["status_code"], 
                                }
           
          
        #This is to handle key error exception, this also gives us the default error
        self.current_err = self.exception_dict.get(type(e), 500)
        
    def parse_curr_err(self, debug = False):
        if self.current_err != 500:  #check for existing error code in the exception, 
            emsg = self.exception.message
        elif self.current_err == 500:
            #This is for debugging original exceptions, if we don't have this, 
            #we will always get internal error, so that end-user can't 
            #see original exceptions
            emsg = "Internal server error"
        return {'status_code' : self.current_err, 'msg' : emsg}


#Provisioning the nodes for a given project using the image and snapshot name
#given. The nodes are typically implemented using ceph.
def provision(node_name, img_name = "hadoopMaster.img",\
        snap_name = "HadoopMasterGoldenImage",\
        debug = False):
    try:
        fsconfig = create_fsconfigobj()
        fs_obj = init_fs(fsconfig)
        if fs_obj.clone(img_name.encode('utf-8'),\
                snap_name.encode('utf-8'),\
                node_name.encode("utf-8")):
            iscsi_output = call_shellscript('./iscsi_update.sh', \
                                        [fsconfig.keyring, \
                                fsconfig.rid, fsconfig.pool, node_name, 'create'])
            if 'successfully' in iscsi_output[0]:
                return ret_200(True)    
            elif 'already' in iscsi_output[0]:
                raise ShellScriptException("Node is already in use")
    except Exception as e:
        return ResponseException(e).parse_curr_err()            
    finally:
        fs_obj.tear_down()

#This is for detach a node and removing it from iscsi
#and destroying its image
def detach_node(node_name):
    try:
        fsconfig = create_fsconfigobj()
        fs_obj = init_fs(fsconfig)
        iscsi_output = call_shellscript('./iscsi_update.sh', \
                                        [fsconfig.keyring, \
                                fsconfig.rid, fsconfig.pool, node_name,\
                                'delete'])
        if 'successfully' in iscsi_output[0]:
            fs_obj._remove(node_name.encode("utf-8"))
            return ret_200(True)
        elif 'already' in iscsi_output[0]:
            raise ShellScriptException("Node already part unmapped and no image exists")
    except Exception as e:
        return ResponseException(e).parse_curr_err() 
    finally:
        fs_obj.tear_down()    


#Creates snapshot for the given image with snap_name as given name
def create_snapshot(img_name, snap_name):
    try:
        fsconfig = create_fsconfigobj() 
        fs_obj = init_fs(fsconfig)
        if fs_obj.init_image(img_name):
            a = ret_200(fs_obj.snap_image(img_name, snap_name))
            fs_obj.tear_down()
            return a
    except Exception as e:
        fs_obj.tear_down()
        return ResponseException(e).parse_curr_err()

#Lists snapshot for the given image img_name 
def list_snaps(img_name):
    try:
        fsconfig = create_fsconfigobj() 
        fs_obj = init_fs(fsconfig)
        if fs_obj.init_image(img_name):
            a = ret_200(fs_obj.list_snapshots(img_name))
            fs_obj.tear_down()
            return a
    except Exception as e:
        fs_obj.tear_down()
        return ResponseException(e).parse_curr_err()

#Removes snapshot sna_name for the given image img_name
def remove_snaps(img_name, snap_name):
    try:
        fsconfig = create_fsconfigobj() 
        fs_obj = init_fs(fsconfig)
        if fs_obj.init_image(img_name):
            a = ret_200(fs_obj.remove_snapshots(img_name, snap_name))
            fs_obj.tear_down()
            return a
    except Exception as e:
        fs_obj.tear_down()
        return ResponseException(e).parse_curr_err()



#Lists the images for the project which includes the snapshot
def list_all_images(debug = False):
    try:
        fsconfig = create_fsconfigobj()
        fs_obj = init_fs(fsconfig)
        a = ret_200(fs_obj.list_n())
        fs_obj.tear_down()
        return a
    except Exception as e:
        return ResponseException(e).parse_curr_err()

#Creates a filesystem configuration object
def create_fsconfigobj():
    fsconfig = GlobalConfig()
    fsconfig.parse_config()    
    return fsconfig


#This function initializes files system object and 
# returns an object for it.
def init_fs(fsconfig, debug = False):
    try:
        if fsconfig.fstype == "ceph":
             return RBD(fsconfig.rid,\
                    fsconfig.r_conf,\
                    fsconfig.pool, debug)
    except Exception as e:
        return ResponseException(e).parse_curr_err()


#A custom function which is wrapper around only success code that
#we are creating.
def ret_200(obj):
    return {"status_code" : 200, "retval" : obj}


###### haas business #####

class HaasRequest(object):
    def __init__(self, method, data):
        self.method = method
        self.data = json.dumps(data)
    def __str__(self):
        return str({"method" : str(self.method),\
                "data" : self.data})

def call_haas(url, req, debug = False):
    ret = call_haas_inner(url, req)
    return ret

def call_haas_inner(url, req):
    if req.method == 'get':
        return requests.get(url)
    if req.method == "post":
            ret = requests.post(url, data=req.data)
            return ret 

def resp_parse(obj, resptype = 1):
    if obj.status_code == 200 and resptype is 1:
        return {"status_code" : obj.status_code, "retval" : obj.json()}

    elif obj.status_code == 200 and resptype is not 1:
        return {"status_code" : obj.status_code}

    elif obj.status_code != 200 and obj.status_code < 400:
        return {"status_code" : obj.status_code}

    elif obj.status_code > 399:
        raise HaasException(errors = {"status_code" : obj.status_code})

def list_free_nodes(haas_url, debug = None):
    try:
        api = 'free_nodes'
        c_api = urlparse.urljoin(haas_url, api)
        haas_req = HaasRequest('get', None)
        if debug:
            print c_api 
        haas_call_ret = call_haas(c_api, haas_req)
        return resp_parse(haas_call_ret)

    except Exception as e:
        return ResponseException(e).parse_curr_err()



def query_project_nodes(haas_url, project):
    api = '/nodes'
    c_api = urlparse.urljoin(haas_url, '/project/' + project +  api)
    haas_req = HaasRequest('get', None)
    haas_call_ret = call_haas(c_api, haas_req)
    try:
        return resp_parse(haas_call_ret)   
    except Exception as e:
        return ResponseException(e).parse_curr_err()
 
def detach_node_from_project(haas_url, project, node, debug = None):
    api = '/detach_node'
    c_api = urlparse.urljoin(haas_url, 'project/' + project + api)
    ret_net_obj = str()
    body = {"node" : node}
    haas_req = HaasRequest('post', body)
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()
          

def attach_node_to_project_network(haas_url, node, nic,\
        network, channel = "vlan/native",\
        debug = None):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/connect_network'
    c_api = urlparse.urljoin(haas_url, api)
    print c_api
    body = {"network" : network, "channel" : channel}
    haas_req = HaasRequest('post', body)
    t_ret= call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def attach_node_haas_project(haas_url,project,node,\
        debug = None):
    api = '/connect_node'
    c_api = urlparse.urljoin(haas_url, 'project/' + project + api)
    ret_obj = list()
    body = {"node" : node}
    haas_req = HaasRequest('post', body)
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def detach_node_from_project_network(haas_url, node,\
        network, nic = 'enp130s0f0', debug = None):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/detach_network'
    c_api = urlparse.urljoin(haas_url, api)
    body = {"network" : network}
    haas_req = HaasRequest('post', body)
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def attach_node_to_project_network(haas_url, node, nic,\
        network, channel = "vlan/native", debug = False):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/connect_network'
    c_api = urlparse.urljoin(haas_url, api)
    body = {"network" : network, "channel" : channel}
    haas_req = HaasRequest('post', body)
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()
    

if __name__ == "__main__":
    print list_free_nodes('http://127.0.0.1:7000/', debug = False)['retval']
    time.sleep(5)
    print attach_node_haas_project('http://127.0.0.1:7000/', "bmi_infra", 'cisco-27', debug = False)
    print "above is attach node to a proj"
    print query_project_nodes('http://127.0.0.1:7000/', project = "bmi_infra")
    time.sleep(5)
    print attach_node_to_project_network('http://127.0.0.1:7000/', 'cisco-27', "enp130s0f0", "bmi-provision", debug = True)
    time.sleep(5)
    print "above is attach network"
    print detach_node_from_project_network('http://127.0.0.1:7000/','cisco-27', 'bmi-provision' ,"enp130s0f0", debug = True)
    time.sleep(5)
    print "above is detach from net"
    print detach_node_from_project('http://127.0.0.1:7000/', project = "bmi_infra", node = 'cisco-27', debug = False)
    time.sleep(5)
    print "above is detach from the proj"
    print query_project_nodes('http://127.0.0.1:7000/', project = "bmi_infra")
    time.sleep(5)
    try:
        raise ShellScriptException("lljl")
    except Exception as e:
        print ResponseException(e).parse_curr_err()
