.notes
======

nembex consist of few separate elements:

* nemUpdate.py - responsible for getting data from servers, analysing it and storing in db
* nemBex3.py - backand for UI (frontend dir contains the frontend ;))
* async/ - contains async-based network crawler


Few notes regarding backend:

* handlers that handle pages have plural forms - (i.e. transfer**s**, multisig**s**, etc.)
* handlers that handle specific things are in signular (i.e. /transfer/)
* python backend always return all the TXes, UI distinguishes if it's part of multisig or not, so you might notice {{#signature}} tags in templates

.deployment
===========

Since nembex consist of 3 separate elements, deployment is not an easy task.

Ofc you will need NIS node too, as APIs that are used are 'local' only.
It is possible to keep nis on a separate node, and grant the access to nembex using ``nis.additionalLocalIps``.

db
--

nembex requires postgresql db.
You'll need to create db for nembex and it's suggested to create separate user.

    CREATE USER nembexuser WITH PASSWORD 'nembexpass';
    CREATE DATABASE nembexdb OWNER nembexuser;

Having that, modify config.py appropriately.

file storage
------------

If you've cloned repository into 'nembex-v3' directory, you'll need to create
directory 'blocks' parallel to 'nembex-v3' directory.

    main-dir
     + nembex-v3
     + blocks

nemUpdate uses that directory to cache the data obtained from a node.
Normaly this data is not used, but at the moment of writing this the caching
cannot be configured.

If you have db and file-storage prepared, and proper nodes set in the config,
you're ready to start nemUpdate.py

webserver
---------

nemBex3.py serves only as a backend for JS app.
By default nemBex3 starts on port 8888, this can be changed with a ``--port=1234`` switch.

The following describes only nginx configuration.

Frontend uses ``/api3`` url, so first thing is to setup proper link to the backend:

in ``server`` context:

    location /api3 {
        proxy_pass http://localhost:8888/api;
        proxy_buffering off;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Scheme $scheme;
        proxy_set_header X-Forwarded-Protocol $scheme;
        proxy_set_header X-Forwarded-Proto http;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

and additionally in ``http`` context:

    upstream frontends {
        server 127.0.0.1:8888;
    }

The port used in both cases is a port on which nembex is started.

You can either point nginx to frontend dir, or copy the content where you want it to be.

async
-----

The node collector needs python3.4. How to start is beyond the scope of this doc.

