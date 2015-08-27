#!/usr/bin/python
from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo
from datetime import datetime, timedelta
from dateutil import parser
import boto.utils
import logging
import logging.handlers
import re
import sys
import time

def get_resource_tags(conn, resource_id):
  resource_tags = {}
  if resource_id:
      tags = conn.get_all_tags({ 'resource-id': resource_id })
      for tag in tags:
          # Tags starting with 'aws:' are reserved for internal use
          if not tag.name.startswith('aws:'):
              resource_tags[tag.name] = tag.value
  return resource_tags

def set_resource_tags(resource, tags):
  for tag_key, tag_value in tags.iteritems():
    if tag_key not in resource.tags or resource.tags[tag_key] != tag_value:
      print 'Tagging %(resource_id)s with [%(tag_key)s: %(tag_value)s]' % {
            'resource_id': resource.id,
            'tag_key': tag_key,
            'tag_value': tag_value
      }
      resource.add_tag(tag_key, tag_value)


def process_region(region, backup_type, retention_days, instanceid):
  conn = boto.ec2.connect_to_region(region)
  volumes = conn.get_all_volumes()
  current_time_str = datetime.strftime(datetime.now(), '%Y-%m-%d-%H%M')

  for v  in volumes:
    # if the volume is not attached, don't take snapshots
    if v.attach_data.status != 'attached':
      continue;

    if (instanceid != None) and (v.attach_data.instance_id != instanceid):
      continue;

    tags = get_resource_tags(conn, v.id)
    ec2_instance_tags = get_resource_tags(conn, v.attach_data.instance_id)
 
    if 'Name' in ec2_instance_tags:
      instance_name = ec2_instance_tags['Name']
    else:
      instance_name = v.attach_data.instance_id

    # if the volume does not have a name, we need to find out a name for the snapshots
    if (not 'Name' in tags):

      if 'Name' in ec2_instance_tags:
        if (v.attach_data.device == '/dev/sda1'):
  	  # We know that this is an unnamed system disk and we have the instance's name so we'll permanently set the name tag
  	  tags['Name'] = "{0}-system".format(ec2_instance_tags['Name'])
	  set_resource_tags(v, tags)
        else:
  	  # We know the system's name but cannot know the purpose of this volume so we temporarily name it by the device it it attached to
	  tags['Name'] = "{0}{1}".format(ec2_instance_tags['Name'], re.sub('[\/]', '-', v.attach_data.device))
      else:
        # The ec2 instance is not named so we need to use the instance id instead
        tags['Name'] = "{0}{1}".format(v.attach_data.instance_id, re.sub('[\/]', '-', v.attach_data.device))

    # Make sure we come up with ANY name for an attached volume, even use the ebs volume if needed
    volume_name = tags['Name'] if 'Name' in tags else str(v.id)

    # We are not interested in backing up the swap volumes, so in case of any 
    if (volume_name.endswith('-swap')):
      continue;

    # Create the snapshot
    snap = conn.create_snapshot(v.id, "{0}_backup_{1}".format(instance_name, current_time_str))
    conn.create_tags(snap.id, { "AutoBackup": "Type={0}|RetentionPeriodDays={1}".format(backup_type, retention_days) })
    log.info("Created snapshot {0} for volume {1}".format(snap.id, volume_name)) 

    # Process all existing snapshots for the current volume and delete all with retention period due
    for snap in v.snapshots():
      snaptags = get_resource_tags(conn, snap.id)
      if not 'AutoBackup' in snaptags:
        continue
      s = snaptags['AutoBackup']
      m = re.search(r'RetentionPeriodDays=(\d+)', s)
      if m:
        retentionDays =  m.group(1)
      else:
        retentionDays = 7

      limit = datetime.now() - timedelta(days=7)
      if (parser.parse(snap.start_time).date() <= limit.date()):
  	if conn.delete_snapshot(snap.id):
	  log.info("Deleted expired snapshot {0} for {1} with RetentionPeriodDays={2} and description {3}".format(snap.id, volume_name, retentionDays, snap.description))
        else:
          log.warn("Failed to delete snapshot {0} for {1} with RetentionPeriodDays={2} and description {3}".format(snap.id, volume_name, retentionDays, snap.description))

##############################################################################

# usage
if ((len(sys.argv) < 3) or (len(sys.argv) > 4)):
    print "USAGE: EC2_SNAPSHOT_WITH_ROTATE.PY <backup_type_description> <num_of_snapshots_to_keep> [instance-id]"
    quit()

# set logging
log = logging.getLogger('ec2_snapshot_with_rotate')
log.setLevel(logging.INFO)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('ec2_snapshot_with_rotate %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

regions = []

# get current region
metadata =  boto.utils.get_instance_metadata()
az = metadata['placement']['availability-zone']
regions.append(az[:-1])

# add other regions here with append if needed
#

#instance
if (len(sys.argv) == 4):
  instanceid = sys.argv[3]
else:
  instanceid = None
  
for region in regions:
  log.info("Processing region {0}".format(region))
  process_region(region, sys.argv[1], sys.argv[2], instanceid)

