DEPLOYMENTS

There are three ways Clarama can be deployed:

Locally - Development - Windows / Linux
---------------------------------------

Runs using the local Flask single user development server

With python requirements installed from requirements.txt. Needs a local postgres install and access to a redis database
Build the Rust apps in source/ clarama-rust
Run dev_launch.bat to run the Rust apps and then run the Clarama app, configured to run everything in one, with
Flask debugging enabled.

 - Individual security access for users when accessing files via their own code in tasks will be compromised

Locally - Scalable - Windows / Linux
------------------------------------

Runs using Waitress webserver in Windows or Linux

With python requirements installed from requirements.txt. Needs a local postgres install and access to a redis database
Build the Rust apps in source/ clarama-rust
Run dev_launch.bat to run the Rust apps and then run the Clarama app, configured to run everything in one, with
Flask debugging enabled.

 - Individual security access for users when accessing files via their own code in tasks will be compromised

Docker - Linux Only
-------------------

runs using gunicorn webserver in Linux

With the built images made by running "make build" and then "make up". Ensure that the ports don't overlap
Environments launched here expect a folder NFS mounted to point to the user's specific set of files using the clarama FTP server.

This ensures that user code executed in Python has the same read/write access as defined in the Clarama security.

Kubernetes - Linux Only
-------------------

Runs using gunicorn webserver in pods on k8s in Linux

default namespace: clarama

Environments launched here expect a folder NFS mounted to point to the user's specific set of files using the clarama FTP server.

This ensures that user code executed in Python has the same read/write access as defined in the Clarama security.





=== Gap Report: bChart vs bChartStream ===
A maintained gap analysis comparing features of the static bChart and the streaming bChartStream is available at:
- README_bchartstream_gap.md (repo root)

This document summarizes unsupported/partial features in the streaming renderer and includes feasibility notes for closing gaps.
