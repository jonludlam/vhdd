#!/usr/bin/env python
import iscsilib
import scsiutil
import demjson
import os
import sys

def probe_iqns(target, port):
    iqns = iscsilib.discovery(target, port, "", "")
    return iqns

def get_portal(target, port, iqn):
    iqns = iscsilib.discovery(target, port, "", "")
    for (portal,tpgt,i) in iqns:
        if iqn == i:
            return portal
        
def get_probe_possibility(path,lunid):
    scsiid=scsiutil.getSCSIid(path)
    size=scsiutil.getsize(path)
    serial=scsiutil.getserial(path)
    vendor=scsiutil.getmanufacturer(path)
    return { 'info' : {'vendor' : vendor,
                       'serial' : serial,
                       'LUNid' : lunid,
                       'size' : "%d" % size,
                       'SCSIid' : scsiid},
             'new_device_config' : {'SCSIid' : scsiid} }
      
def probe_luns(target, port, iqn):
    portal = get_portal(target, port, iqn)
    iscsilib.login(portal, iqn, "", "")
    try:
        luns = iscsilib.get_luns(iqn, portal)
        return [get_probe_possibility(os.path.join("/dev/iscsi/",iqn,portal,"LUN%s" % lun),lun) for lun in luns]
    finally:
        iscsilib.logout(portal, iqn)

def attach(target, port, iqn, scsiid):
    portal = get_portal(target, port, iqn)
    iscsilib.login(portal, iqn, "", "")
    try:
        luns = iscsilib.get_luns(iqn, portal)
        infos = [get_probe_possibility(os.path.join("/dev/iscsi/",iqn,portal,"LUN%s" % lun),lun) for lun in luns]
        for info in infos:
            if info['device_config']['SCSIid'] == scsiid:
                return os.path.join("/dev/iscsi",iqn,portal,"LUN%s" % lun)
        raise "Unknown SCSIid"
    except:
        iscsilib.logout(portal, iqn)
        raise

def detach(target, port, iqn):
    portal = get_portal(target, port, iqn)
    iscsilib.logout(portal,iqn)

def main():
    if sys.argv[1] == "attach":
        target=sys.argv[2]
        port=sys.argv[3]
        iqn=sys.argv[4]
        scsiid=sys.argv[5]
        result=attach(target,port,iqn,scsiid)
    if sys.argv[1] == "detach":
        target=sys.argv[2]
        port=sys.argv[3]
        iqn=sys.argv[4]
        result=detach(target,port,iqn)
    if sys.argv[1] == "probe_iqns":
        target=sys.argv[2]
        port=sys.argv[3]
        result=probe_iqns(target,port)
    if sys.argv[1] == "probe_luns":
        target=sys.argv[2]
        port=sys.argv[3]
        iqn=sys.argv[4]
        result=probe_luns(target,port,iqn)
    print demjson.encode(result)
    
if __name__ == "__main__":
    main()
    
