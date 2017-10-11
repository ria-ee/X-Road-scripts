# X-Road-scripts

This repository contains helper scripts that can simplify usage and
administration of X-Road.

Provided scripts support TLS authentication with Security Server. Self-signed
key and certificate can be created with openssl:
```
openssl req -x509 -sha256 -nodes -days 365 -newkey rsa:2048 -keyout privateKey.key -out certificate.crt
```
If Security Server requires TLS authentication then you can add your TLS
certificate in Security Server administration interface: MEMBER/SUBSYSTEM ->
Internal Servers -> INTERNAL TLS CERTIFICATES

## Messagelog:
[cat_mlog.sh](messagelog/cat_mlog.sh) - This script prints the contents of archived
X-Road messagelog files to STDOUT. Therefore providing a way to "grep" the
contents of messagelog files.

[cat_mlog_st.sh](messagelog/cat_mlog_st.sh) - Slower version of cat_mlog.sh that
additionally outputs message SigningTime.

## Global Configuration and Metadata services
[xrdinfo.py](xrdinfo/xrdinfo.py) - Python module that can be imported and used in
any Python2.7+ or Python3 application. It implements:
* loading of global configuration from Security Server, Central Server or
  Configuration Proxy;
* parsing of shared_params.xml;
* listMethods and allowedMethods X-Road requests;
* getWsdl X-Road request.

There ara also some example scripts that use xrdinfo module:
* [xrd_all_methods.py](xrdinfo/xrd_all_methods.py) - Returns the list of all
  methods (services) in X-Road instance.
* [xrd_all_methods_allowed.py](xrdinfo/xrd_all_methods_allowed.py) - Returns
  the list of all allowed methods in X-Road instance. Or in other words
  services with access rights granted to the issuer of the X-Road request.
* [xrd_methods.py](xrdinfo/xrd_methods.py) - Returns the list of methods
  provided by a specified X-Road Member.
* [xrd_methods_allowed.py](xrdinfo/xrd_methods_allowed.py) - Returns the list
  of allowed methods provided by a specified X-Road Member.
* [xrd_registered_subsystems.py](xrdinfo/xrd_registered_subsystems.py) -
  Returns the list of all Subsystems in X-Road instance that are registered
  in at least one Security Server.
* [xrd_servers_ips.py](xrdinfo/xrd_servers_ips.py) - Returns the list of IP
  addresses of all Security Servers in X-Road instance. Can be used to
  configure Security Servers firewall.
* [xrd_servers.py](xrdinfo/xrd_servers.py) - Returns the list of all Security
  Servers in X-Road instance.
* [xrd_subsystems.py](xrdinfo/xrd_subsystems.py) - Returns the list of all
  Subsystems in X-Road instance.
* [xrd_subsystems_with_membername.py](xrdinfo/xrd_subsystems_with_membername.py) -
  Returns the list of all Subsystems in X-Road instance and additionally adding
  Member names to that list.
* [xrd_subsystems_with_server.py](xrdinfo/xrd_subsystems_with_server.py) - 
  Returns the list of all Subsystems in X-Road instance and additionally adding
  Security Servers to that list.
* [xrd_wsdl.py](xrdinfo/xrd_wsdl.py) - Returns the service WSDL using X-Road
  request. This script can be useful when http://SECURITYSERVER/wsdl service is
  disabled or TLS authentication is required to access Security Server.

Warning are disabled by default. Set environmental variable `XRDINFO_DEBUG=True`
or use the following command after importing xrdinfo to enable warnings:
```python
xrdinfo.DEBUG=True
```

## Health and Environment monitoring
[metrics.py](zabbix/metrics.py) - X-Road Health and Environment monitoring
collector for Zabbix. Can be used by:
* Central monitoring to collect Environmental and Health data about all
  Security Servers in X-Road instance.
* Security Server owners to collect Environmental data of their Security Server.
* X-Road members to collect other members Health data.

[zabbix_cron.sh](zabbix/zabbix_cron.sh) - Sample shell script that can be
executed from crontab to periodically collect the data.

Use the provided examples to create your configuration file:
* [metrics.cfg_example](zabbix/metrics.cfg_example) - Example configuration
  file.
* [metrics.cfg_local_example](zabbix/metrics.cfg_local_example) - Example
  configuration file for local Security Server monitoring.
