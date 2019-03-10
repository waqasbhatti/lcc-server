This directory contains example systemd .service files for the LCC-Server main
process and the accompanying checkplotserver process.

To use these:

- Edit the files and fill in `{{ lccserver_basedir }}` and `{{ lccserver_venv }}`.
- Since we'll be running them in `--user` mode, make sure there is a
  `/home/{{ username }}/.config/systemd/user` directory. If not, make one using
  `mkdir -p` if necessary.
- Put these files in that directory.
- Run `systemd --user daemon-reload`.
- Enable the authnzerver service: `systemctl --user enable authnzerver`
  and launch it using `systemctl --user start authnzerver`.
- Enable the checkplotserver service: `systemctl --user enable checkplotserver`
  and launch it using `systemctl --user start checkplotserver`.

To see the logs in the systemd journal, use: `journalctl --user-unit
checkplotserver [or authnzerver]` or `systemctl --user status checkplotserver
[or authnzerver]` and verify that they launched properly.

You should also execute: `sudo loginctl enable-linger {{ username }}` for the
user that the LCC-Server will run under. This will make the services launch on
system boot and persist after you've logged out.

The `lcc-server@.service` file is templated so you can launch multiple instances
of the server as needed:

```bash
# enable the service
systemctl --user enable lcc-server@.service

# launch an instance listening on port 12500
systemctl --user start lcc-server@12500

# launch an instance listening on port 12501
systemctl --user start lcc-server@12501

# see logs for instance running on 12501
journalctl --user-unit lcc-server@12501.service

# restart lcc-server listening on 12500
systemctl --user restart lcc-server@12500.service

# stop lcc-server listening on 12500
systemctl --user stop lcc-server@12500.service

# etc.
```

This is useful for use with upstream nginx load-balancing:

```
upstream tornado-lcc-server {

    server 127.0.0.1:12500 fail_timeout=100s;
    server 127.0.0.1:12501 fail_timeout=100s;

}
```

Then in your `location` directive, use something like:

```
    # deny all dotfiles
    location ~ /\. { deny  all; }

    location = /robots.txt {
             alias {{ lccserver_basedir }}/docs/robots.txt;
    }

    location / {

             proxy_pass http://tornado-lcc-server;
             proxy_http_version 1.1;

             proxy_set_header Proxy "";
             proxy_set_header X-Forwarded-For $remote_addr;
             proxy_set_header X-Real-IP $remote_addr;
             proxy_set_header X-Forwarded-Proto $scheme;
             proxy_set_header X-Real-Host $host;
    }
```
