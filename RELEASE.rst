=======================
Angus Service Framework
=======================

Release Note 0.0.15
+++++++++++++++++++

Bug fixes
--------------

* Fix input stream. At end of stream, chunk data was not well parsed.


Release Note 0.0.14
+++++++++++++++++++

Features added
--------------

* Add possibility to log flat json documents coming from services in order to store/index them later on


Release Note 0.0.13
+++++++++++++++++++

Features added
--------------

* Remove cassandra based quota


Release Note 0.0.11
+++++++++++++++++++

Bug fixes
---------

* Fix lag in service due to quota backend service.


Release Note 0.0.10
+++++++++++++++++++

Features added
--------------

* Streaming interface (MJPEG)

Bug fixes
---------

* Test file exists before remove it in resources object.
* Fix RELEASE.rst


Release Note 0.0.9
++++++++++++++++++

Features added
--------------

* Quota support.

Bug fixes
---------

* Server Async call request result fix.


Release Note 0.0.1
++++++++++++++++++

Features added
--------------

* Angus.ai service framework library for python.
* Resource, Jobs, JobCollection, Service: building blocks for create
  some IA web services in a RESTful style.
