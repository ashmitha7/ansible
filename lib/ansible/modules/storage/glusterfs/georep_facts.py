#!/usr/bin/python
# -*- coding: utf-8 -*-


ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: georep_facts

short_description: module to get status of a geo-replication session.

version_added: "2.4"

description:
    - "module helps to get status and volume name of geo-replication session in gluster enabled hosts"

options:
    status:
        description:to get the status of geo replication sessions in the hosts. With the status options we get information such as the
                    master node, slave user name and the names of the master and slave volumes etc.
        required: true
author:
    - Ashmitha Ambastha (@ashmitha7)
'''
EXAMPLES = '''
# To get status
 - name: get status of a geo-rep session
   geo_rep: action=status

#the module will get information such as volume names, nodes and slave user name from action=status task and then run the tasks to
#start and stop geo-rep sessions.


'''
RETURN = '''
MASTER VOL- <master_volume name>
SLAVE-<for example, ssh://10.11.77.12::volume1>
'''

import sys
import re
import shlex
from subprocess import Popen
import subprocess
import os


from collections import OrderedDict
from ansible.module_utils.basic import *
from ast import literal_eval

class GeoRep(object):
    def __init__(self, module):
        self.module = module
        self.action = self._validated_params('action')
        self.gluster_georep_ops()

    def get_playbook_params(self, opt):
        return self.module.params[opt]

    def _validated_params(self, opt):
        value = self.get_playbook_params(opt)
        if value is None:
            msg = "Please provide %s option in the playbook!" % opt
            self.module.fail_json(msg=msg)
        return value

    def gluster_georep_ops(self):
        if self.action == 'get_vol_data':
            return self.gluster_status_vol()

    def gluster_status_vol(self):
        cmd_str="gluster volume geo-replication status --xml"
        try:
            cmd = Popen(
                shlex.split(cmd_str),
                stdin=open(os.devnull, "r"),
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                close_fds=True
            )
            op, err = cmd.communicate()
            dict_georep={}
            info = ElementTree.fromstring(op)
            it = info.iter("volume")
            georep_dict = {}
            try:
                while True:
                    volume = it.next()
                    # georep_dict[volume.find("name").text] = []
                    vol_sessions = volume.iter("sessions")
                    try:
                        while True:
                            vol_session = vol_sessions.next()
                            session_it = vol_session.iter("session")
                            try:
                                while True:
                                    session = session_it.next()
                                    session_slave_val = session.find("session_slave").text
                                    georep_dict[volume.find("name").text] = session_slave_val
                            except StopIteration:
                                pass
                    except StopIteration:
                        pass
            except StopIteration:
                pass
            # print georep_dict
            master_vols = []
            slave_vols = []
            for k,v in georep_dict.iteritems():
                s_value = v.split("//")[1].split(":")
                #slave_vol = '::'.join([s_value[0],s_value[2]])
                #master_vol= k
                master_vols.append(k)
                slave_vols.append('::'.join([s_value[0],s_value[2]]))
                #dict_georep['slavevol']=slave_vol
                #dict_georep['mastervol']=master_vol
                # self.module.exit_json(rc=0,msg=dict_georep)
            dict_georep["mastervol"] = master_vols
            dict_georep["slavevol"] = slave_vols
            # print dict_georep


            # def VolumeInfo(opt):
            root = ElementTree.fromstring(opt)
            volumes_dict = {}
            for volume in root.findall('volInfo/volumes/volume'):
                value_dict = {}
                value_dict['volumeName'] = volume.find('name').text
                value_dict['replicaCount'] = volume.find('replicaCount').text
                value_dict['volumeType'] = volume.find('typeStr').text.replace('-', '_')
                volumes_dict[value_dict['volumeName']] = value_dict
            # print volumes_dict
            # return volumes_dict

            # def VolumeGeoRepStatus(volumeName, op):
            slaves = {}
            for volumeName in volumes_dict:
                 if "Replicate" in volumes_dict[volumeName]["volumeType"]:
                     repCount = int(volumes_dict[volumeName]["replicaCount"])
                    #  print repCount
                 else:
                     repCount = 1
            tree = ElementTree.fromstring(op)
            volume = tree.find('geoRep/volume')
            other_status = ['active', 'initializing']

            for session in volume.findall('sessions/session'):
                session_slave = session.find('session_slave').text
                slave = session_slave.split("::")[-1]
                slaves[slave] = {'nodecount': 0,
                                 'faulty': 0,
                                 'notstarted': 0,
                                 'stopped': 0,
                                 'passive': 0,
                                 'detail': '',
                                 'status': 'GeoRepStatus.OK',
                                 'name': session_slave.split(":", 1)[1]
                                 }
                for pair in session.findall('pair'):
                    slaves[slave]['nodecount'] += 1
                    status = pair.find('status').text
                    # print status
                    tempstatus = None
                    if "faulty" in status:
                        slaves[slave]['faulty'] += 1
                        # print tempstatus
                    elif "created" in status:
                        slaves[slave]['notstarted'] += 1
                        tempstatus = "notstarted"
                        # print tempstatus
                    elif "passive" in status:
                        slaves[slave]['passive'] += 1
                        tempstatus = "passive"
                    elif "stopped" in status:
                        slaves[slave]['stopped'] += 1
                        tempstatus = "stopped"
                    elif status not in other_status:
                        tempstatus = status
                if slaves[slave]['faulty'] > 0:
                    if repCount > 1:
                        if (slaves[slave]['faulty'] + slaves[slave]['passive']
                                > slaves[slave]['nodecount']/repCount):
                            slaves[slave]['status'] = "faulty"
                        else:
                            slaves[slave]['status'] = "partial_faulty"
                    else:
                        slaves[slave]['status'] = "faulty"
                elif (slaves[slave]['notstarted'] > 0 and
                      slaves[slave]['status'] == "ok"):
                    slaves[slave]['status'] ="notstarted"
                elif (slaves[slave]['stopped'] > 0 and
                      slaves[slave]['status'] == "ok"):
                      slaves[slave]['status'] = "Stopped"

                def nested_get(_dict, keys, default=None):
                    def _reducer(d,key):
                        if isinstance(d,dict):
                            return d.get(key,default)
                        return default
                    return reduce(_reducer, keys, _dict)

                status_list = {volumeName: {'slaves': slaves}}
                status_value =  nested_get(status_list,['vol_1','slaves','volume1:f8b13afd-9cdd-4b90-98ff-72cbcfc98927','status'])
                # print status_value
                status_present = 'status'
                dict_georep[status_present]=status_value
                print dict_georep
                self.module.exit_json(rc=0,msg=dict_georep)
        except (subprocess.CalledProcessError, ValueError):
            print "Error....."

    def call_gluster_cmd(self, *args):
        params = ' '.join(opt for opt in args)
        return self._run_command('gluster', ' ' + params )

    def _get_output(self, rc, output, err):
        carryon = False
        changed = 0 if (carryon and rc) else 1
        if not rc or carryon:
            self.module.exit_json(stdout=output, changed=changed)
        else:
            self.module.fail_json(msg=err)

    def _run_command(self, op, opts):
        cmd = self.module.get_bin_path(op, True) + opts
        return self.module.run_command(cmd)

def run_module():
    module = AnsibleModule(
        argument_spec=dict(
        action=dict(required=True, choices=['status','get_vol_data']),
        ),
    )
    GeoRep(module)

try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree

def main():
    run_module()

if __name__ == '__main__':
    main()
