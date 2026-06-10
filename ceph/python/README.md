## cephfs_file_info.py

Gets information on a file in CephFS by querying the Ceph APIs.

Run `./cephfs_file_info.py -h` to show usage information.

### Current Limitations

- A Ceph configuration file can't be specified, so has to be run as root or in a setup where the Ceph python libraries can find the information needed to connect to the API and authenticate successfully.
- It only supports CephFS setups where stripe count is 1 (object size == stripe unit).
