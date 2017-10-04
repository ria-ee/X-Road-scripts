# X-Road-scripts

This repository contains helper scripts that can simplify usage and
administration of X-Road.

## Messagelog:
[cat_mlog.sh](shell/cat_mlog.sh) - This script prints the contents of archived
X-Road messagelog files to STDOUT. Therefore providing a way to "grep" the
contents of messagelog files.

[cat_mlog_st.sh](shell/cat_mlog_st.sh) - Slower version of cat_mlog.sh that
additionally outputs message SigningTime.

## Global Configuration and Metadata services
[xrdinfo](python/xrdinfo) - Python package that can be imported and used in
any Python2.7+ or Python3 application. It implements:
* loading of global configuration from Security Server, Central Server or
  Configuration Proxy;
* parsing of shared_params.xml;
* listMethods and allowedMethods X-Road requests.

There ara also some example scripts that use xrdinfo package:
* [xrd_all_methods.py](python/xrd_all_methods.py) - Returns the list of all
  methods (services) in X-Road instance.
* [xrd_all_methods_allowed.py](python/xrd_all_methods_allowed.py) - Returns
  the list of all allowed methods in X-Road instance. Or in other words
  services with access rights granted to the issuer of the X-Road request.
* [xrd_methods.py](python/xrd_methods.py) - Returns the list of methods
  provided by a specified X-Road Member.
* [xrd_methods_allowed.py](python/xrd_methods_allowed.py) - Returns the list
  of allowed methods provided by a specified X-Road Member.
* [xrd_registered_subsystems.py](python/xrd_registered_subsystems.py) -
  Returns the list of all Subsystems in X-Road instance that are registered
  in at least one Security Server.
* [xrd_servers_ips.py](python/xrd_servers_ips.py) - Returns the list of IP
  addresses of all Security Servers in X-Road instance. Can be used to
  configure Security Servers firewall.
* [xrd_servers.py](python/xrd_servers.py) - Returns the list of all Security
  Servers in X-Road instance.
* [xrd_subsystems.py](python/xrd_subsystems.py) - Returns the list of all
  Subsystems in X-Road instance.
* [xrd_subsystems_with_membername.py](python/xrd_subsystems_with_membername.py) -
  Returns the list of all Subsystems in X-Road instance and additioally adding
  Member names to that list.
* [xrd_subsystems_with_server.py](python/xrd_subsystems_with_server.py) - 
  Returns the list of all Subsystems in X-Road instance and additioally adding
  Security Servers to that list.

Warning are disabled by default. Use the following command after importing
xrdinfo to enable warnings:
```python
xrdinfo.DEBUG=True
```
