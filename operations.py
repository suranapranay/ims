#!/usr/bin/python
import sys
from ceph_wrapper import *
import ConfigParser, os
import subprocess
import os.path
import urlparse
from urlparse import urlsplit
import requests
import json
import time
from dbtry import *
#As of now a shell script that can accomodate rbd map and update.
#Since there are no python rbd map and updates wrappers around ceph.
class ShellScriptException(Exception): 
    def __init__(self, message = None, errors = None):
        super(ShellScriptException, self).__init__(message)
        if not errors:
            errors = {"status_code" : -1}  # this sets the default and unused error code.
        self.errors = errors                # we need this because we hash this and refer to error 
                                            
class HaasException(Exception): 
    def __init__(self, message = None, errors = None):
        super(HaasException, self).__init__(message)
        self.errors = errors 
        print errors
        if not errors:
            self.errors = {"status_code" : -1}  # this sets the default and unused error code.
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
        if not hasattr(self.exception, 'errors'): #slightly hackish initializations,\
# but necessary so that the dictionary can be initialized.
            self.exception.errors = {"status_code" : -1}
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
           type(DbException()) : self.exception.errors["status_code"], 
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

# provision : when map filename to num, create num files instead of image_name.
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
            raise ShellScriptException("Node already"\
                    " part unmapped and no image exists")
    except Exception as e:
        return ResponseException(e).parse_curr_err() 
    finally:
        fs_obj.tear_down()    


#Creates snapshot for the given image with snap_name as given name
def create_snapshot(url, usr, passwd, project, img_name, snap_name):
    try:
        fs_done = False 
        db_done = False 
        if not check_auth(url, usr, passwd, project):
            raise HaasException(errors = {"status_code" : 401})
        dbo = DbObject() ## gets the db part done. Just run queries! :)
        p_entry  = dbo.session.query(dbo.Project).filter_by(project_name\
                = project).first()
# None as a query result implies that the project does not exist.
        if p_entry is None:
            raise DbException(message = "No such Project",\
                errors = {"status_code" : 491})
        i_entry = dbo.session.query(dbo.Images).filter_by(pid = p_entry.pid).filter_by(filename = img_name).first()
        if i_entry is None:
            raise DbException(message = "No such Image",\
                errors = {"status_code" : 492})
        s_entry = dbo.session.query(dbo.Snaps).filter_by(filenum = i_entry.filenum).filter_by(snap_name = snap_name).first()
        if s_entry is not None:
            raise DbException(message = "Snap already exists",\
                errors = {"status_code" : 493})
        new_snap = dbo.Snaps(filenum = i_entry.filenum, snap_name = snap_name)       
        dbo.session.add(new_snap)
        dbo.session.flush()
        db_done = True 
        # MAPPINGS ## Since we change image_name to filenum that we get
        # And we keep snap_name the same.
        ## REDIFINE img_name as str(filenum) :) and we are set. 


        # VIRTUALIZATION HAPPENS HERE.
        user_img_name = img_name
        img_name = str(i_entry.filenum)
        #### REMAP DONE




        fsconfig = create_fsconfigobj() 
        fs_obj = init_fs(fsconfig)
        
        if fs_obj.init_image(img_name):
            a = ret_200(fs_obj.snap_image(img_name, snap_name))
            fs_done = True
            dbo.session.commit()
            db_commited = True
            fs_obj.tear_down()
            return a
    except Exception as e:
        ## if db is not commited, but file operations are done
        if fs_done and not db_commited:
            fs_obj.tear_down()
            remove_snaps(url, usr, passwd, project, snap_name, user_img_name) 
        dbo.session.rollback()
        return ResponseException(e).parse_curr_err()
    finally:
            dbo.destroy()
#Lists snapshot for the given image img_name 
#URL's have to be read from BMI config file
def list_snaps(url, usr, passwd, project, img_name):
    try:
        if not check_auth(url, usr, passwd, project):
            raise HaasException(errors = {"status_code" : 401})
        dbo = DbObject() ## gets the db part done. Just run queries! :)
        p_entry  = dbo.session.query(dbo.Project).filter_by(project_name\
                = project).first()
# None as a query result implies that the project does not exist.
        if p_entry is None:
            raise DbException(message = "No such Project",\
                errors = {"status_code" : 491})
        i_entry = dbo.session.query(dbo.Images).filter_by(pid = p_entry.pid).filter_by(filename = img_name).first()
        if i_entry is None:
            raise DbException(message = "No such Image",\
                errors = {"status_code" : 492})
        s_entries = dbo.session.query(dbo.Snaps).filter_by(filenum = i_entry.filenum).all()
        snap_list = [s_entry.snap_name for s_entry in s_entries] 
        #fsconfig = create_fsconfigobj() 
        #fs_obj = init_fs(fsconfig)
        #if fs_obj.init_image(img_name):
            #a = ret_200(fs_obj.list_snapshots(img_name))
        a = ret_200(snap_list)
            #fs_obj.tear_down()
        return a
    except Exception as e:
        #fs_obj.tear_down()
        return ResponseException(e).parse_curr_err()
    finally:
        dbo.destroy()
#Removes snapshot sna_name for the given image img_name
def remove_snaps(url, usr, passwd, project, img_name, snap_name):
    try:
        if not check_auth(url, usr, passwd, project):
            raise HaasException(errors = {"status_code" : 401})
        dbo = DbObject() ## gets the db part done. Just run queries! :)
        p_entry  = dbo.session.query(dbo.Project).filter_by(project_name\
                = project).first()
# None as a query result implies that the project does not exist.
        if p_entry is None:
            raise DbException(message = "No such Project",\
                errors = {"status_code" : 491})
        i_entry = dbo.session.query(dbo.Images).filter_by(pid = p_entry.pid).filter_by(filename = img_name).first()
        if i_entry is None:
            raise DbException(message = "No such Image",\
                errors = {"status_code" : 492})
        s_entries = dbo.session.query(dbo.Snaps).filter_by(filenum = i_entry.filenum).all()
        for s_entry in s_entries:
            dbo.session.delete(s_entry)
        dbo.session.flush() 
        fsconfig = create_fsconfigobj() 
        fs_obj = init_fs(fsconfig)
        user_img_name = img_name
        img_name = str(i_entry.filenum)
        if fs_obj.init_image(img_name):
            a = ret_200(fs_obj.remove_snapshots(img_name, snap_name))
            dbo.session.commit()
            fs_obj.tear_down()
            return a
    except Exception as e:
        if  "fs_obj" in locals():
            fs_obj.tear_down()
        if snap_name not in list_snaps(url, usr, passwd, project, user_img_name):
            create_snapshot(url, usr, passwd, project, snap_name, user_img_name) 
        dbo.session.rollback()
        dbo.session.commit()
        return ResponseException(e).parse_curr_err()
    finally:
        dbo.destroy()



#Lists the images for the project which includes the snapshot
def list_all_images(url, usr, passwd, project):
    try:
        if not check_auth(url, usr, passwd, project):
            raise HaasException(errors = {"status_code" : 401})
        return get_image_list_from_db(project) 
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
    def __init__(self, method, data, auth = None):
        self.method = method
        self.data = json.dumps(data)
        self.auth = None
        if auth:
            self.auth = auth 
    def __str__(self):
        return str({"method" : str(self.method),\
                "data" : self.data, "auth" : self.auth})

def call_haas(url, req, debug = False):
    ret = call_haas_inner(url, req)
    if debug:
        print req
    return ret

def call_haas_inner(url, req):
    if req.method == 'get':
        return requests.get(url, auth = req.auth)
    if req.method == "post":
            ret = requests.post(url, data = req.data, auth = req.auth)
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

def list_free_nodes(haas_url, usr, passwd,  debug = None):
    try:
        api = 'free_nodes'
        c_api = urlparse.urljoin(haas_url, api)
        haas_req = HaasRequest('get', None, auth = (usr, passwd))
        if debug:
            print c_api 
        haas_call_ret = call_haas(c_api, haas_req)
        return resp_parse(haas_call_ret)

    except Exception as e:
        return ResponseException(e).parse_curr_err()



def query_project_nodes(haas_url, project, usr, passwd):
    api = '/nodes'
    c_api = urlparse.urljoin(haas_url, '/project/' + project +  api)
    haas_req = HaasRequest('get', None, auth = (usr, passwd))
    haas_call_ret = call_haas(c_api, haas_req)
    try:
        return resp_parse(haas_call_ret)   
    except Exception as e:
        return ResponseException(e).parse_curr_err()
 
def detach_node_from_project(haas_url, project, node, usr, passwd,  debug = None):
    api = '/detach_node'
    c_api = urlparse.urljoin(haas_url, 'project/' + project + api)
    ret_net_obj = str()
    body = {"node" : node}
    haas_req = HaasRequest('post', body, auth = (usr, passwd))
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()
          

def attach_node_to_project_network(haas_url, node, nic,\
        network,usr, passwd, channel = "vlan/native",\
        debug = None):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/connect_network'
    c_api = urlparse.urljoin(haas_url, api)
    body = {"network" : network, "channel" : channel}
    haas_req = HaasRequest('post', body, auth = (usr, passwd))
    t_ret= call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def attach_node_haas_project(haas_url,project,node,\
        usr, passwd, debug = None):
    api = '/connect_node'
    c_api = urlparse.urljoin(haas_url, 'project/' + project + api)
    ret_obj = list()
    body = {"node" : node}
    haas_req = HaasRequest('post', body, auth = (usr, passwd))
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def detach_node_from_project_network(haas_url, node,\
        network, usr, passwd, nic = 'enp130s0f0', debug = None):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/detach_network'
    c_api = urlparse.urljoin(haas_url, api)
    body = {"network" : network}
    haas_req = HaasRequest('post', body, auth = (usr, passwd) ) 
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()

def attach_node_to_project_network(haas_url, node, nic,\
        network,usr, passwd, channel = "vlan/native", debug = False):
    ret_obj = list()
    api = '/node/' + node + '/nic/' + nic + '/connect_network'
    c_api = urlparse.urljoin(haas_url, api)
    body = {"network" : network, "channel" : channel}
    haas_req = HaasRequest('post', body, auth = (usr, passwd))
    t_ret = call_haas(c_api, haas_req, debug)
    if debug:
        print {"url" : c_api, "node" : node, "nic" : nic}
    try:
        return resp_parse(t_ret, resptype = 2)
    except Exception as e:
        return ResponseException(e).parse_curr_err()
    
def check_auth(haas_url, usr, passwd, project):
    api = '/nodes'
    c_api = urlparse.urljoin(haas_url, '/project/' + project +  api)
    haas_req = HaasRequest('get', None, auth = (usr, passwd))
    ret = call_haas(c_api, haas_req)
    if ret.status_code == 401:
        return False
    return True 


if __name__ == "__main__":
    print check_auth('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'bmi_penultimate')
    print list_all_images('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'test')
    print list_all_images('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'asdfsadf')
    print list_snaps('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'bmi_penultimate','testimage' )
    #print create_snapshot('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'bmi_penultimate','testimage', 'blblb1')
    print remove_snaps('http://127.0.0.1:6500/', "haasadmin", "admin1234", 'bmi_penultimate','testimage', 'blblb1')
    '''
    print list_free_nodes('http://127.0.0.1:6500/', "haasadmin", "admin1234",  debug = True)['retval']
    time.sleep(5)

    print attach_node_haas_project('http://127.0.0.1:6500/', "bmi_penultimate", 'sun-12',\
            usr = "haasadmin", passwd = "admin1234", debug = True)
    print "above is attach node to a proj"
    
    print query_project_nodes('http://127.0.0.1:6500/',  "bmi_penultimate", "haasadmin", "admin1234")
    time.sleep(5)
    
    print attach_node_to_project_network('http://127.0.0.1:7000/', 'cisco-27',\
            "enp130s0f0", "bmi-provision","test", "test",  debug = True)
    time.sleep(5)
    print "above is attach network"

    print detach_node_from_project_network('http://127.0.0.1:7000/','cisco-27',\
            'bmi-provision', "test", "test", "enp130s0f0", debug = True)
    time.sleep(5)

    
    print "above is detach from net"
    print detach_node_from_project('http://127.0.0.1:6500/',\
              "bmi_penultimate", 'sun-12',  usr = "haasadmin", passwd = "admin1234",  debug = True)
    time.sleep(5)
    print "above is detach from the proj"
    print query_project_nodes('http://127.0.0.1:6500/', "bmi_penultimate", "haasadmin", "admin1234")
    time.sleep(5)
    try:
        raise ShellScriptException("lljl")
    except Exception as e:
        print ResponseException(e).parse_curr_err()
    ''' 
