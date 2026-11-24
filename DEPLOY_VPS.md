# Deploy the SPP data pipeline on a VPS

This procedure installs the pipeline on one Ubuntu VPS. The VPS runs:

- Airflow (Astro CLI and Docker): the SPP DA, RT and hourly load jobs, and the
  Beatrice scraper. The Beatrice HMI is on the internet, so the VPS does not
  need a VPN.
- Postgres 16: the warehouse database (`SPP_DSN`).

The Garden City scraper does not run on the VPS. It runs on the device that
has the site VPN, and it writes to the Postgres database on the VPS (step 12).

A non-root user (`spp` in this procedure) owns the project and runs all
pipeline commands. Use `sudo` only for the system steps: packages, Postgres
configuration, firewall and the hosts file.

For the VPS size, refer to README → [VPS configuration](README.md#vps-configuration).

## Contents

1. [Make the non-root user](#1-make-the-non-root-user)
2. [Install the system packages](#2-install-the-system-packages)
3. [Install Docker](#3-install-docker)
4. [Install the Astro CLI](#4-install-the-astro-cli)
5. [Install Postgres](#5-install-postgres)
6. [Set the firewall](#6-set-the-firewall)
7. [Get the code and set the configuration](#7-get-the-code-and-set-the-configuration)
8. [Make the database](#8-make-the-database)
9. [Start Airflow](#9-start-airflow)
10. [Make a virtual environment for manual commands](#10-make-a-virtual-environment-for-manual-commands)
11. [Load SPP data for a calendar range](#11-load-spp-data-for-a-calendar-range)
12. [Connect the Garden City device](#12-connect-the-garden-city-device)
13. [Operation and maintenance](#13-operation-and-maintenance)
14. [Troubleshooting](#14-troubleshooting)
15. [Branches, CI and the nightly deploy](#15-branches-ci-and-the-nightly-deploy)

---

## 1. Make the non-root user

Do this step one time, as `root` or as the default user of the provider.

```bash
sudo adduser spp                    # set a strong password
sudo usermod -aG sudo spp
# copy your SSH public key to the new user
sudo mkdir -p /home/spp/.ssh
sudo cp ~/.ssh/authorized_keys /home/spp/.ssh/
sudo chown -R spp:spp /home/spp/.ssh && sudo chmod 700 /home/spp/.ssh
```

Open a new SSH session as `spp` and make sure that it works:

```bash
ssh spp@<VPS_IP>
sudo whoami                         # shows: root
```

Do all the subsequent steps as `spp`.

**Recommended:** stop SSH login for `root`. In `/etc/ssh/sshd_config`, set
`PermitRootLogin no`, then run `sudo systemctl restart ssh`. Do this only after
you make sure that the login as `spp` works.

## 2. Install the system packages

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git make tmux \
     python3-venv python3-pip postgresql-client
sudo timedatectl set-timezone America/Chicago   # optional: logs in Central time
```

The pipeline does not need the VPS time zone. All time calculations use
`America/Chicago` in the code. The optional setting only makes the shell and
cron times easier to read.

## 3. Install Docker

Use the Docker repository (the Ubuntu `docker.io` package is older):

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
     docker-buildx-plugin docker-compose-plugin
```

Let `spp` use Docker without `sudo`:

```bash
sudo usermod -aG docker spp
```

Log out and log in again. Then make sure that Docker works:

```bash
docker run --rm hello-world
```

> **Security note:** a member of the `docker` group can get root access
> through Docker. Give this group only to the `spp` user.

## 4. Install the Astro CLI

```bash
curl -sSL install.astronomer.io | sudo bash -s
astro version
```

The installer puts `astro` in `/usr/local/bin`. You run all `astro` commands
as `spp`, not with `sudo`.

The Astro CLI starts its own Postgres container for the Airflow metadata.
Recent versions of the Astro CLI give each project its own ports (for
example 14329 for this database), so there is no conflict with the warehouse
Postgres on 5432. An older version uses 5432. If `astro dev start` stops with
"port 5432 is in use", move the Astro port:

```bash
astro config set -g postgres.port 5435
```

## 5. Install Postgres

Ubuntu 24.04 has Postgres 16:

```bash
sudo apt-get install -y postgresql
```

Make the database user and the database:

```bash
sudo -u postgres createuser --pwprompt spp     # use a strong password
sudo -u postgres createdb -O spp spp
```

### Let the Airflow containers connect

The containers connect to the host as `host.docker.internal`
(`docker-compose.override.yml` in the project adds this name). Docker gives
the containers addresses in `172.16.0.0/12`.

1. In `/etc/postgresql/16/main/postgresql.conf`, set:

   ```
   listen_addresses = '*'
   ```

   The firewall (step 6) and `pg_hba.conf` control which clients can connect.

2. In `/etc/postgresql/16/main/pg_hba.conf`, add this line at the end:

   ```
   # Airflow containers on this VPS
   host    spp    spp    172.16.0.0/12    scram-sha-256
   ```

3. Optional tuning for a VPS with 8 GB of memory that also runs Airflow. Put
   these values in `postgresql.conf`:

   ```
   shared_buffers = 1GB
   effective_cache_size = 3GB
   work_mem = 16MB
   maintenance_work_mem = 256MB
   ```

   For 16 GB of memory, double `shared_buffers` and `effective_cache_size`.

4. Restart Postgres:

   ```bash
   sudo systemctl restart postgresql
   ```

### Use the same DSN on the host and in the containers

In the containers, `host.docker.internal` is the VPS. On the VPS itself, the
name does not exist. Add it to the hosts file of the VPS, so the same
`SPP_DSN` in `.env` works on the host and in the containers:

```bash
echo "127.0.0.1 host.docker.internal" | sudo tee -a /etc/hosts
```

## 6. Set the firewall

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
# Airflow containers -> Postgres on the host
sudo ufw allow from 172.16.0.0/12 to any port 5432 proto tcp
# Garden City device -> Postgres (step 12). Use its public IP address.
sudo ufw allow from <GARDEN_CITY_IP> to any port 5432 proto tcp
sudo ufw enable
sudo ufw status verbose
```

> **WARNING:** ports that Docker publishes do not go through `ufw`. The Astro
> stack publishes the Airflow UI and its metadata database on
> all interfaces. The local Airflow UI of the Astro CLI does not ask for a
> login. Thus you must also set the **firewall of the VPS provider** (cloud
> firewall or security group) to allow only:
>
> - TCP 22 from your addresses.
> - TCP 5432 from the Garden City device.
>
> Do not open the Astro ports (for example 6563, 8080, 5435, or the ports in
> `astro dev ps`). Use an SSH tunnel for the UI (step 9). To see which ports
> listen on all interfaces, run `sudo ss -tlnp`.

## 7. Get the code and set the configuration

The repository is private (`akala47/spp_pipeline` on GitHub). The VPS gets
the code with a **deploy key**: an SSH key that gives access to this one
repository only. Use a read-only deploy key. The VPS must not push changes.

### 7.1 Make a deploy key on the VPS

Do these commands as `spp`:

```bash
mkdir -p ~/.ssh && chmod 700 ~/.ssh
ssh-keygen -t ed25519 -N "" -C "spp-vps deploy key" -f ~/.ssh/spp_pipeline_deploy
chmod 600 ~/.ssh/spp_pipeline_deploy
```

The key has no passphrase, so that `git pull` can run with no person present.
Do not copy the private key (`~/.ssh/spp_pipeline_deploy`) to a different
computer.

Tell SSH to use this key for this repository only. Add these lines to
`~/.ssh/config`:

```bash
cat >> ~/.ssh/config <<'EOF'

# GitHub deploy key: akala47/spp_pipeline (read-only)
Host github-spp
    HostName github.com
    User git
    IdentityFile ~/.ssh/spp_pipeline_deploy
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config
```

`github-spp` is a name for this key only. The Git remote uses this name in
place of `github.com` (step 7.3).

### 7.2 Add the public key to GitHub

1. Show the public key and copy the full line:

   ```bash
   cat ~/.ssh/spp_pipeline_deploy.pub
   ```

2. On GitHub, open the repository `akala47/spp_pipeline`. You must be an
   administrator of the repository.
3. Go to **Settings → Deploy keys → Add deploy key**.
4. Set **Title** to the VPS name, for example `spp-vps-<hostname>`.
5. Paste the public key into **Key**.
6. Do **not** select **Allow write access**.
7. Click **Add key**.

Make sure that the VPS knows the real GitHub host key. Get the fingerprint,
then compare it with the ED25519 fingerprint on the GitHub documentation page
"GitHub's SSH key fingerprints":

```bash
ssh-keyscan -t ed25519 github.com 2>/dev/null | tee -a ~/.ssh/known_hosts | ssh-keygen -lf -
```

If the fingerprints are not the same, stop. Do not continue.

Make sure that the key works:

```bash
ssh -T git@github-spp
# Hi akala47/spp_pipeline! You've successfully authenticated, but GitHub does not provide shell access.
```

### 7.3 Clone the repository

The VPS runs the `prod` branch (step 15). The default branch on GitHub is
`dev`, so give the branch:

```bash
cd ~
git clone -b prod git@github-spp:akala47/spp_pipeline.git spp_data_pipeline
cd ~/spp_data_pipeline
git config pull.ff only           # a pull never makes a merge commit on the VPS
git status                        # On branch prod, up to date with 'origin/prod'
```

The nightly deploy (step 15) gets the later versions. Do not edit tracked
files on the VPS: the deploy stops when it finds a changed file.

### 7.4 Set the configuration

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env`. Set at minimum:

| Variable | Value on the VPS |
|---|---|
| `SPP_DSN` | `postgresql://spp:<password>@host.docker.internal:5432/spp` |
| `BEATRICE_USER`, `BEATRICE_PWD`, `BEATRICE_LOGIN_URL`, `BEATRICE_SVG_URL` | The Beatrice HMI values |
| `SC_LOGIN_LINK`, `SC_USER`, `SC_PWD` | The Sherburne login page, email and password |
| `SCADA_CHROME_BINARY` | `/usr/bin/chromium` |
| `SCADA_CHROMEDRIVER` | `/usr/bin/chromedriver` |

Do not set the `GC_*` values on the VPS. Do not put spaces around `=` in
`.env`. If a value contains `&`, `?`, `$`, `#` or a space, put it in single
quotes (for example `SPP_DSN='postgresql://…?sslmode=require&…'`). Without
quotes, `. ./.env` in step 8 cuts the value at the `&`.

Make the log folder for manual runs:

```bash
mkdir -p ~/spp_data_pipeline/logs
```

## 8. Make the database

Run the two schema scripts one time:

```bash
cd ~/spp_data_pipeline
set -a; . ./.env; set +a                 # loads SPP_DSN into the shell
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/01_schema.sql
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/02_seed.sql
```

`02_seed.sql` adds the spine rows and monthly partitions from 2019-01-01 to
2027-12-31. To use a different range, add `-v seed_from=YYYY-MM-DD -v
seed_to=YYYY-MM-DD`. You can run `02_seed.sql` again at any time.

Make sure that the tables exist:

```bash
psql "$SPP_DSN" -c '\dt'
psql "$SPP_DSN" -c 'SELECT count(*) FROM timestamps'
```

> **Dev database only:** to delete everything and start again, run
> `make reset-dev SPP_DSN="$SPP_DSN"`. Do not do this on a database with data
> that you must keep.

## 9. Start Airflow

### First start

```bash
cd ~/spp_data_pipeline
astro dev start --no-browser
astro dev ps                              # all containers must show "Up"
```

The first start builds the image. It installs Chromium and chromedriver from
`packages.txt`. This can take 5 to 10 minutes.

Make sure that the containers can connect to the database:

```bash
astro dev bash
python -m spp.run --feed rt_lmp --check-db
python -m spp.run --feed rt_lmp --list-groups
python -m spp.scada.beatrice --dry-run    # one Beatrice reading, no write
exit
```

### Open the Airflow UI

The ports depend on the Astro CLI version. Find them on the VPS:

```bash
cd ~/spp_data_pipeline && astro dev ps        # PORTS column of api-server
journalctl --user -u spp-airflow.service -b --no-pager | grep 'Airflow UI'
```

- **Recent Astro CLI:** the log shows
  `Airflow UI: http://spp-data-pipeline.localhost:6563`. A local proxy
  (`astro dev proxy`) listens on port 6563. The API server has its own port
  (for example `13506` in `astro dev ps`).
- **Older Astro CLI:** the log shows `http://localhost:8080`. The API server
  listens on port 8080.

Make sure that the UI answers on the VPS itself. Use the port of the API
server:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:<API_PORT>/
# 200 or 302 = OK
```

Then open an SSH tunnel from your computer. Use one of these two methods.

**Method A: tunnel to the API server (all browsers).** Your computer uses
port 8080, and the VPS uses the API server port:

```bash
ssh -N -L 8080:127.0.0.1:<API_PORT> spp@<VPS_IP>      # for example <API_PORT> = 13506
```

Open `http://localhost:8080`.

**Method B: tunnel to the Astro proxy.** Use the same port on both sides:

```bash
ssh -N -L 6563:127.0.0.1:6563 spp@<VPS_IP>
```

Open `http://spp-data-pipeline.localhost:6563`. The proxy selects the
project from this name, so `http://localhost:6563` does not work. Chrome and
Firefox send `*.localhost` to your computer. If your browser does not, use
method A.

Use `127.0.0.1`, not `localhost`, on the right side of `-L`: `localhost` can
resolve to the IPv6 address `::1`, and the tunnel then fails.

If the browser shows "connection reset", the tunnel works but nothing
answers on that port of the VPS. Examine the port (above) and refer to
"After a reboot" below.

### Set the node groups and unpause the DAGs

```bash
cd ~/spp_data_pipeline
astro dev run variables set spp_rt_lmp_groups nppd,kmea
astro dev run variables set spp_da_lmp_groups nppd,kmea

for d in spp_rt_lmp spp_rt_mcp spp_day_ahead spp_gap_sweeper scada_beatrice scada_sherburne; do
    astro dev run dags unpause "$d"
done
```

`spp_hourly_load` has `catchup=True` and a start date of 2026-01-01. When you
unpause it, Airflow starts one run for each day from 2026-01-01. If you load
that range with step 11, you do not need these runs. Unpause it when you want
the scheduled runs:

```bash
astro dev run dags unpause spp_hourly_load
```

### Start Airflow when the VPS starts

Install the systemd **user** unit. It runs as `spp`, not as root:

```bash
mkdir -p ~/.config/systemd/user
cp ~/spp_data_pipeline/deploy/systemd/spp-airflow.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable spp-airflow.service
sudo loginctl enable-linger spp           # one time: start the unit without a login
```

Make sure that linger is on. Without linger, the unit starts only when `spp`
logs in, not when the VPS starts:

```bash
loginctl show-user spp --property=Linger      # must show Linger=yes
```

If you installed the unit before this version, copy it again and reload
(`systemctl --user daemon-reload`). The current unit waits for Docker and
tries again after a failure.

### After a reboot

The stack needs time to start. Docker starts first, then the unit runs
`astro dev start`. The Airflow API server answers approximately 2 to 5
minutes after the VPS starts. During this time, the tunnel shows
"connection reset".

1. Log in as `spp` and look at the unit:

   ```bash
   systemctl --user status spp-airflow.service
   journalctl --user -u spp-airflow.service -b --no-pager | tail -n 40
   ```

   - `active (exited)`: the start is complete. Go to step 2.
   - `activating (start)`: the start continues. Wait, then do this step again.
   - `failed`: read the journal lines. Correct the cause, then run
     `systemctl --user restart spp-airflow.service`.
   - `Unit spp-airflow.service could not be found`: install the unit (above).

2. Look at the containers:

   ```bash
   cd ~/spp_data_pipeline && astro dev ps
   ```

   All containers must show `Up`. If `api-server` shows `Restarting` or
   `Exited`, look at its log: `docker logs --tail 50 $(docker ps -aqf name=api-server)`.
   A frequent cause is too little memory (refer to README → VPS
   configuration).

3. Make sure that the API server answers on the VPS itself:

   ```bash
   curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:<API_PORT>/
   # <API_PORT> from `astro dev ps`, or 8080 on an older Astro CLI.
   # 200 or 302 = OK. "Connection refused" or "reset" = not ready or wrong port.
   ```

   When this command shows 200 or 302, the tunnel also works.

## 10. Make a virtual environment for manual commands

You can run manual commands in the Airflow container (`astro dev bash`). For
long jobs (backfills), use a virtual environment on the host. A job on the
host continues when you restart Airflow.

```bash
cd ~/spp_data_pipeline
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e .
.venv/bin/python -m spp.run --feed rt_lmp --check-db
```

The package reads `.env` from the project folder. Because of the hosts entry
in step 5, the same `SPP_DSN` works on the host.

## 11. Load SPP data for a calendar range

### Dates: operating day and run day

`--start` and `--end` are **operating days**: the dates of the prints. Both
days are included.

| You want | Scheduled run that loads it | Command |
|---|---|---|
| DA prints for November 2 | The run on November 1 (run day + 1) | `--feed da_lmp --start 2026-11-02` |
| Hourly load prints for October 31 | The run on November 1 (run day − 1) | `--feed hourly_load --start 2026-10-31` |
| RT intervals of November 1 | The runs on November 1 | `--feed rt_lmp --start 2026-11-01` |

To give run days, add `--by-run-date`. For example, `--feed da_lmp --start
2026-11-01 --by-run-date` loads the November 2 prints.

For an RT day, the command loads the 288 intervals that start at 00:00,
00:05, …, 23:55 Central (spine ids `…HB00MB00` to `…HB23MB55`).

### Procedure

1. Start a `tmux` session, so the job continues if your SSH session stops:

   ```bash
   tmux new -s backfill
   cd ~/spp_data_pipeline
   ```

2. Load the daily feeds. These feeds have one file for each day and are fast:

   ```bash
   scripts/backfill.sh da_lmp,da_mcp,hourly_load 2025-01-01 2025-12-31
   ```

3. Load the RT feeds in bounded windows. Each RT feed has 288 files for each
   day. Start with one month and look at the time that it takes:

   ```bash
   scripts/backfill.sh rt_lmp,rt_mcp 2026-10-01 2026-10-31
   ```

4. To load only some nodes or zones, add the options of `spp.run`:

   ```bash
   scripts/backfill.sh rt_lmp 2026-10-01 2026-10-31 --groups nppd
   scripts/backfill.sh rt_mcp 2026-10-01 2026-10-31 --members 1,5,SPP
   ```

5. Optional: forward fill the gaps that remain:

   ```bash
   scripts/backfill.sh rt_lmp,rt_mcp 2026-10-01 2026-10-31 --fill
   ```

   A fill does not get files from SPP. It copies the last earlier value of the
   same node or zone into each missing interval, and marks the row
   `source='forward_filled'`. A later load of published data replaces it.

6. Disconnect from `tmux` with `Ctrl-b d`. To connect again:
   `tmux attach -t backfill`.

`scripts/backfill.sh` divides the range into calendar months. It writes one
log file for each feed and month in `logs/backfill/`. At the end, it shows
the windows that failed. All writes are upserts, so you can run a failed
window again:

```bash
scripts/backfill.sh rt_lmp 2026-10-01 2026-10-31
```

For one day or a short range, you can also use `spp.run` directly:

```bash
.venv/bin/python -m spp.run --feed da_lmp --start 2026-11-02
.venv/bin/python -m spp.run --feed hourly_load --start 2026-10-01 --end 2026-10-31 --no-strict
```

Use `--no-strict` for past data. Without it, the row-count check stops on a
short day, for example a DST change day (23 or 25 hours).

### Make sure that the data is complete

```bash
psql "$SPP_DSN" <<'SQL'
-- rows for each day in October 2026 (288 x number of nodes for a full RT day)
SELECT t.cal_date, count(*)
  FROM rt_lmp f JOIN timestamps t ON t.id = f.timestamp_id
 WHERE f.timestamp_id >= spine_id('2026-10-01') AND f.timestamp_id < spine_id('2026-11-01')
 GROUP BY 1 ORDER BY 1;
SQL
```

After a large backfill, update the statistics and the visibility map:

```bash
psql "$SPP_DSN" -c 'VACUUM (ANALYZE) rt_lmp' -c 'VACUUM (ANALYZE) rt_mcp'
```

### Cost of an RT backfill

Each RT file is approximately 175 KB. A full range from 2019 is approximately
800,000 requests and 140 GB of download for each RT feed. It takes
approximately twelve days for each feed. Before you start a large range, make
sure that SPP still has the oldest files:

```bash
curl -sI "https://portal.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=%2F2019%2F01%2FBy_Interval%2F01%2FRTBM-LMP-SL-201901010005.csv" | head -1
```

A backfill and the scheduled DAGs can run at the same time. The upserts
prevent duplicate rows.

## 12. Connect the Garden City device

The Garden City scraper runs on the device that has the site VPN. It writes
to the Postgres database on the VPS through the internet, with TLS.

### On the VPS

1. Add the device to `/etc/postgresql/16/main/pg_hba.conf`. Use `hostssl`, so
   the connection must use TLS:

   ```
   # Garden City device
   hostssl spp    spp    <GARDEN_CITY_IP>/32    scram-sha-256
   ```

2. Reload Postgres:

   ```bash
   sudo systemctl reload postgresql
   ```

3. Make sure that step 6 allows port 5432 from this address, in `ufw` and in
   the firewall of the provider.

Ubuntu turns on TLS in Postgres by default (with a self-signed certificate).
For a certificate that the client can verify, refer to the Postgres
documentation (`ssl_cert_file`, `ssl_key_file`).

### On the Garden City device

1. Get the code and make a virtual environment. Use a separate deploy key
   for this device: do steps 7.1 to 7.3 on the device, with a different key
   title (for example `spp-garden-city`). GitHub accepts more than one deploy
   key for a repository.

   ```bash
   git clone -b prod git@github-spp:akala47/spp_pipeline.git spp_data_pipeline
   cd spp_data_pipeline
   python3 -m venv .venv && .venv/bin/pip install -e '.[scada]'
   cp .env.example .env && chmod 600 .env
   ```

2. In `.env`, set:

   ```
   SPP_DSN=postgresql://spp:<password>@<VPS_IP>:5432/spp?sslmode=require
   GC_SVG_LINK=...      # the Garden City load SVG page
   ```

   Remove `SCADA_CHROME_BINARY` and `SCADA_CHROMEDRIVER` if Chrome is in the
   normal location on the device.

3. Connect the site VPN. Then do a test:

   ```bash
   .venv/bin/python -m spp.scada.garden_city --once --dry-run   # read only
   .venv/bin/python -m spp.scada.garden_city --once             # one row
   ```

4. Start the loop. On a Mac, use the launchd unit (README → SCADA readings).

## 13. Operation and maintenance

### Daily commands

| Task | Command (as `spp`, in `~/spp_data_pipeline`) |
|---|---|
| Container status | `astro dev ps` |
| Scheduler log | `astro dev logs -f -s` |
| Restart after a change to `.env` | `astro dev restart` |
| Stop / start | `astro dev stop` / `astro dev start --no-browser` |
| Last SCADA readings | `psql "$SPP_DSN" -c "SELECT * FROM mw_updates ORDER BY reading_timestamp DESC LIMIT 10"` |
| Disk space | `df -h /` and `docker system df` |

### Update the code

The nightly deploy updates the code each night from the `prod` branch
(step 15). It also updates the host virtual environment
(`.venv/bin/pip install -e .`). To deploy now, without waiting for midnight:

```bash
cd ~/spp_data_pipeline
deploy/vps/deploy.sh --dry-run    # shows what it would do; changes nothing
deploy/vps/deploy.sh --no-wait    # deploys now (does not wait for the quiet point)
```

The script applies the same checks as the nightly run (green CI, schema
patches, pre-flight) and rolls back if the health check fails. Do not use
`git pull` + `astro dev restart` by hand: that skips the checks.

If the deploy stops with "tracked files were changed on the VPS", a file on
the VPS was changed. Do not edit tracked files on the VPS. To see the
changes, run `git status`. To discard them, run `git restore .`. `.env` is
not tracked, so these commands do not change it.

To remove the access of a VPS, delete its key on GitHub (**Settings → Deploy
keys**).

Airflow reads changes to the files in `dags/` without a restart.

### Clean the Airflow logs and metadata

The pipeline makes approximately 2,600 task runs each day. Remove old task
logs and old metadata rows each week. Add these lines with `crontab -e` (as
`spp`):

```
# Sunday 03:10: remove task logs older than 14 days
10 3 * * 0  docker exec $(docker ps -qf name=scheduler) find /usr/local/airflow/logs -type f -mtime +14 -delete
# Sunday 03:20: remove metadata rows older than 30 days
20 3 * * 0  cd $HOME/spp_data_pipeline && /usr/local/bin/astro dev run db clean --clean-before-timestamp "$(date -d '-30 days' +\%F)" --skip-archive --yes
# each hour: remove backfill logs older than 30 days
5 * * * *   find $HOME/spp_data_pipeline/logs -type f -mtime +30 -delete
```

### Back up the database

Make a password file, so `pg_dump` does not ask for the password:

```bash
echo "localhost:5432:spp:spp:<password>" > ~/.pgpass && chmod 600 ~/.pgpass
mkdir -p ~/backups
```

Add a nightly backup with `crontab -e`:

```
# 02:40 each night: dump the database; keep 14 days
40 2 * * *  pg_dump -h localhost -U spp -Fc -f $HOME/backups/spp_$(date +\%F).dump spp && find $HOME/backups -name 'spp_*.dump' -mtime +14 -delete
```

Copy the backups to a different location (for example object storage). A
backup on the same VPS does not protect you if you lose the VPS.

To restore into an empty database:

```bash
pg_restore -h localhost -U spp -d spp --clean --if-exists ~/backups/spp_<date>.dump
```

## 14. Troubleshooting

| Problem | Possible cause | Action |
|---|---|---|
| `astro dev start` fails: port 5432 is in use | The Astro metadata database uses the same port as the warehouse Postgres | `astro config set -g postgres.port 5435`, then start again |
| `--check-db` in the container: connection refused or timeout | Postgres does not listen on the Docker network, or `ufw` blocks it | Check `listen_addresses`, `pg_hba.conf` (step 5) and `sudo ufw status` (step 6) |
| `--check-db` on the host: name not found | No hosts entry for `host.docker.internal` | Do the last part of step 5 |
| `scada_beatrice` fails: `session not created` or `chrome not reachable` | `SCADA_CHROME_BINARY` or `SCADA_CHROMEDRIVER` is not set | Set both in `.env` (step 7), then `astro dev restart` |
| `scada_beatrice` fails: `tab crashed` | `/dev/shm` is too small | Make sure that `docker-compose.override.yml` is in the project folder, then `astro dev restart` |
| An RT task skips | SPP did not publish the file in the retry time | `spp_gap_sweeper` loads it later. Or run `spp.run` for that day. |
| `quality.short` in the log for a daily feed | DST change day, or SPP published a short file | For past data, use `--no-strict` |
| A setting in `.env` has no effect | Space before `=`, or no restart | Remove the space, then `astro dev restart` |
| `SPP_DSN` is empty after `. ./.env` | The value contains `&` and has no quotes | Put the value in single quotes |
| Disk is almost full | Airflow task logs, Docker images, backups | Step 13: clean the logs; `docker system prune`; move old backups |
| `git clone` or `git pull`: `Permission denied (publickey)` | The deploy key is not on GitHub, or the remote does not use `github-spp` | Do step 7.2 again. `git remote -v` must show `git@github-spp:akala47/spp_pipeline.git`. |
| Tunnel to the UI: "connection reset" | The tunnel goes to a port where nothing listens (the Astro CLI does not use 8080), or the API server is not running yet | Step 9, "Open the Airflow UI": find the API server port with `astro dev ps` and tunnel to it |
| `git clone` checks out the wrong branch | The default branch on GitHub is `dev` | Clone with `-b prod`, or run `git checkout prod` |
| The nightly deploy did not deploy | A gate stopped it | `tail -n 5 ~/.local/state/spp-deploy/history.tsv` shows the reason. Step 15, "Read the result" |

## 15. Branches, CI and the nightly deploy

### How it works

| Branch | Use |
|---|---|
| `dev` | You develop and test here, on your computer. Default branch on GitHub. |
| `prod` | What the VPS runs. Change it only by a pull request from `dev`. |

`main`, `Upgrade-qa` and `working` are older branches. Nothing deploys them.

1. You push to `dev`. GitHub Actions runs CI (`.github/workflows/ci.yml`):
   - **pytest**: all the tests, with no database (`SPP_DSN` and
     `SPP_TEST_DSN` are empty, so the database tests skip);
   - **DAG import (Astro image)**: builds the Docker image from `Dockerfile`
     and imports every file in `dags/` with the same Airflow as the VPS;
   - **shellcheck deploy scripts**: lints `deploy/vps/*.sh` and runs
     `deploy/vps/test_deploy.sh`. That script runs `deploy.sh` against a fake
     `origin` with stub `astro`, `docker` and `curl`, through each path
     (deploy, rollback, each gate). You can run it on your computer too.
2. When CI is green, you open a pull request `dev` → `prod` and merge it.
   CI runs again on the `prod` commit.
3. At **00:00 Central** each night, the timer `spp-deploy.timer` on the VPS
   starts `deploy/vps/deploy.sh`. The script:
   1. Runs `git fetch origin prod`. If there is nothing new, it stops.
   2. Checks the gates. If one gate fails, it skips the deploy, and Airflow
      keeps running the old code:
      - `prod` is a fast-forward of the commit on the VPS (no force push);
      - the commit did not fail a deploy before;
      - a `ci.yml` run **succeeded for this exact commit** (GitHub API);
      - each new file in `schema/patches/` is in your list of applied
        patches. The deploy never runs SQL. You apply patches yourself;
      - **pre-flight**: the new Docker image builds, and every DAG file
        imports in it. No restart happens yet.
   3. Waits for the **quiet point**: 00:14:45. Then it runs
      `git merge --ff-only` and `astro dev restart`.
   4. Checks the health: the scheduler container runs, the scheduler job is
      alive, there are no DAG import errors, and every DAG from the
      pre-flight is loaded. The check waits up to 10 minutes.
   5. If the health check fails, the script **rolls back** to the previous
      commit, restarts again and checks again. It adds the failed commit to
      `failed_shas`, so the next night does not try the same commit again.

#### Why 00:14:45 and not 00:00

No minute is free of work: `scada_beatrice` runs every minute, and each
`scada_sherburne` run reads for about 14.5 minutes from :00, :15, :30 and
:45. The least harmful moment is just after a Sherburne cycle stops reading
(about :14:30). The RT runs at :12 are done by then, and the next RT runs are
at :17. The slow work (fetch, CI check, image build) happens before this
moment, so the restart itself is short (usually 1 to 3 minutes).

**Expect each deploy to cost:** 1 to 3 Beatrice readings, and a late start
of the 00:15 Sherburne cycle. The RT run at 00:17 can also start late.
`spp_gap_sweeper` loads any RT file that a late run misses. `astro dev
restart` stops the tasks that are running. If the pre-flight takes longer
than 14 minutes (for example, a new package in `requirements.txt`), the
script waits for the next quiet point (00:29:45). A night with nothing new
on `prod` does not restart anything.

To use a different quiet point, set `DEPLOY_QUIET_OFFSET` (seconds into each
quarter hour; default `885`).

### One-time setup on GitHub (on your computer)

1. Make the branches from `main` and push them:

   ```bash
   git fetch origin
   git branch prod origin/main
   git branch dev origin/main
   git push origin prod dev
   ```

2. Make `dev` the default branch:

   ```bash
   gh repo edit akala47/spp_pipeline --default-branch dev
   ```

3. Push a commit that contains `.github/workflows/ci.yml` to `dev`, and
   merge it to `prod`. CI runs only after this file is on the branch.
   Look at **Actions** on GitHub: the three jobs must be green.

4. Protect `prod`: **Settings → Branches → Add branch ruleset** (or **Add
   rule**), branch name `prod`:
   - Require a pull request before merging.
   - Require status checks to pass: `pytest`, `DAG import (Astro image)`,
     `shellcheck deploy scripts`. These names appear after CI has run once.
   - Block force pushes.

5. Make a token for the CI gate. The repository is private, so the VPS needs
   a token to read the CI results. **Settings (your account) → Developer
   settings → Personal access tokens → Fine-grained tokens → Generate new
   token**:
   - Token name: `spp-vps-deploy`
   - Expiration: the longest that you can use. Put the date in your
     calendar. When the token expires, the deploys stop (the log shows
     `GitHub API call failed`).
   - Repository access: **Only select repositories** → `akala47/spp_pipeline`
   - Permissions → Repository permissions → **Actions: Read-only**. Nothing
     else (GitHub adds Metadata: Read-only).

   Copy the token. You use it in the next part. Do not put it in the
   repository or in `.env`.

### One-time setup on the VPS (as `spp`)

1. Move the checkout from `main` to `prod`:

   ```bash
   cd ~/spp_data_pipeline
   git status                                   # must show no changed files
   git fetch origin prod
   git checkout -b prod --track origin/prod     # same commit as main: no restart needed
   ```

   If `prod` is ahead of what runs now, run `deploy/vps/deploy.sh --no-wait`
   after step 3 instead of a manual restart.

2. Put the token in the deploy configuration:

   ```bash
   mkdir -p ~/.config/spp-deploy && chmod 700 ~/.config/spp-deploy
   ( umask 077; read -rsp 'GitHub token: ' t; echo; printf 'GITHUB_TOKEN=%s\n' "$t" > ~/.config/spp-deploy/deploy.env )
   ```

   Optional settings in the same file (one `KEY=value` per line; the file is
   not run as shell code):

   | Setting | Default | Use |
   |---|---|---|
   | `DEPLOY_QUIET_OFFSET` | `885` | Seconds into the quarter hour for the restart. `none` = at once. |
   | `DEPLOY_HEALTH_TIMEOUT` | `600` | Seconds that the health check waits. |
   | `DEPLOY_BRANCH` | `prod` | Branch to deploy. |
   | `PROJECT_DIR` | `~/spp_data_pipeline` | Project folder. |

3. Do a dry run. It fetches, checks CI and the patches, and builds the
   image, but it does not restart Airflow:

   ```bash
   cd ~/spp_data_pipeline
   deploy/vps/deploy.sh --dry-run
   ```

   With nothing new on `prod`, it shows `noop: already at origin/prod`.

4. Install the timer:

   ```bash
   cp deploy/systemd/spp-deploy.service deploy/systemd/spp-deploy.timer ~/.config/systemd/user/
   systemctl --user daemon-reload
   systemctl --user enable --now spp-deploy.timer
   systemctl --user list-timers spp-deploy.timer     # NEXT shows the next 00:00 Central (in the VPS time zone)
   ```

   Linger must be on (step 9), or the timer does not run when you are not
   logged in.

### Daily work (on your computer)

```bash
git checkout dev
# ... change and test ...
SPP_DSN= SPP_TEST_DSN= pytest -q        # the same tests as CI, no database
git push origin dev
# when CI is green on dev:
gh pr create --base prod --head dev --title "Release $(date +%F)" --fill
gh pr merge --merge                      # after the checks pass
```

To run the database tests too, use a throwaway local Postgres. The script
makes a new database in a temp folder (port 55432), loads
`schema/01_schema.sql` and `02_seed.sql`, sets `SPP_DSN` and `SPP_TEST_DSN`
to it, runs `pytest`, then deletes it. It needs the Postgres server programs
(`brew install postgresql@16`; version 14 also works):

```bash
make test-local-db                                  # or: scripts/test_local_db.sh
scripts/test_local_db.sh tests/test_scada.py -k mw  # pytest options
KEEP_DB=1 scripts/test_local_db.sh                  # keep the database to look at it
```

> **WARNING:** `spp/config.py` reads `.env`. If your `.env` sets
> `SPP_TEST_DSN`, a plain `pytest` runs the database tests against that
> database, and the tests write to `SPP_DSN` when it is set. They write and
> delete rows. Use `SPP_DSN= SPP_TEST_DSN= pytest` (no database) or
> `make test-local-db`. Never put a Neon or VPS DSN in `SPP_TEST_DSN`.

A schema change: put the SQL in `schema/patches/YYYY-MM-DD_name.sql`. Before
the night of the deploy, apply it on the VPS, then tell the deploy:

```bash
cd ~/spp_data_pipeline
set -a; . ./.env; set +a
git fetch origin prod                    # the patch is on origin/prod, not yet in the folder
git show origin/prod:schema/patches/YYYY-MM-DD_name.sql | psql "$SPP_DSN" -v ON_ERROR_STOP=1
echo YYYY-MM-DD_name.sql >> ~/.local/state/spp-deploy/applied_patches
```

Until then, the deploy skips each night with `new schema patch(es) not
applied`.

### Read the result

```bash
tail -n 5 ~/.local/state/spp-deploy/history.tsv          # one line for each night
tail -n 80 ~/.local/state/spp-deploy/deploy.log          # details
journalctl --user -u spp-deploy.service -n 80 --no-pager
systemctl --user --failed                                # shows spp-deploy.service after a rollback
```

| Result in `history.tsv` | Meaning | Action |
|---|---|---|
| `noop` | Nothing new on `prod` | None |
| `deployed` | New code runs, health check OK | None |
| `skipped` … `CI is not green` | CI failed, is still running, or did not run for the commit | Fix CI. A run that finishes after midnight deploys the next night. |
| `skipped` … `schema patch` | A new patch is not in `applied_patches` | Apply it (above) |
| `skipped` … `pre-flight failed` | The image does not build, or a DAG does not import | Fix on `dev`, merge again |
| `skipped` … `failed a deploy before` | This commit was rolled back before | Push a fix. To try the same commit again, delete its line from `~/.local/state/spp-deploy/failed_shas`. |
| `skipped` … `not a fast-forward` | Somebody force-pushed `prod` | Check `prod`. To take it for one run: `DEPLOY_ALLOW_NON_FF=1 deploy/vps/deploy.sh --no-wait` |
| `rolled-back` (exit 1) | The new code failed the health check. The old code runs again. | Read `deploy.log`, fix on `dev` |
| `rollback-failed` (exit 2) | The old code is not healthy either | Act now: step 9 "After a reboot" and `astro dev ps` |
| `error` (exit 3) | Wrong branch, changed files on the VPS, or `git fetch` failed | Read the message |

### Stop the nightly deploy

```bash
systemctl --user disable --now spp-deploy.timer
```
