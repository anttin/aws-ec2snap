# aws-ec2snap
Tool for taking ec2 snapshots with rotation.

#prerequisites

```
pip install python-dateutil
```

#usage

```
USAGE: EC2_SNAPSHOT_WITH_ROTATE.PY <backup_type_description> <num_of_snapshots_to_keep>
```

example:

```
./ec2_snapshot_with_rotate.py Daily 7
```

