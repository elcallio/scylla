import json
import yaml
from lib.scylla_cloud import get_cloud_instance, is_ec2

def estimate_streaming_bandwidth():
    cloud_instance = get_cloud_instance()
    disk_bw = 0
    net_bw = 0
    if is_ec2():
        instance_type = cloud_instance.instancetype
        with open('/opt/scylladb/scylla-machine-image/aws_net_params.json') as f:
            netinfo = json.load(f)
            instance_info = [info for info in netinfo if info[0] == instance_type]
            if len(instance_info) != 0:
                net_bw = int(instance_info[0][2] * 1024 * 1024 * 1024) #GBPS
        if net_bw != 0:
            # todo. duplicated from io_setup
            with open('/opt/scylladb/scylla-machine-image/aws_io_params.yaml') as f:
                io_params = yaml.safe_load(f)
                t = cloud_instance.instance_class() + '.ALL'
                if instance_type in io_params:
                    t = instance_type
                disk_bw = min(io_params[t]["read_bandwidth"], io_params[t]["write_bandwidth"])
    # TODO: other clouds

    if net_bw == 0 or disk_bw == 0:
        return 0

    return min(disk_bw, .75 * net_bw) / (1024*1024)

