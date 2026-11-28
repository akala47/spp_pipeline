# Security audit: spp_data_pipeline

Date: 2026-11-23. Source: `main` at `c91a9d0` (clean worktree).
Method: the `security-audit` skill (`security-audit-skill/skills/security-audit`),
**`quick` profile, partial pass**. Structured records: `findings.json` in
`~/security-audit-skill/spp_data_pipeline/run-1/`, checked with `validate-findings.cjs`
(6 records, PASS).

## 1. Scope and limits

- **In scope:** `dags/`, `spp/` (including the standalone `spp/scada/garden_city.py` and
  `spp/scada/sherburne_city.py`), `schema/`, `Dockerfile`, `docker-compose.override.yml`,
  `deploy/`, `DEPLOY_VPS.md`, `README.md`, `requirements.txt`, `pyproject.toml`,
  `packages.txt`, `scripts/`, `tests/`, and the git history of all refs.
- **Source review only.** No code was run. Nothing was sent to the databases, the vendor
  HMIs or the VPS. The local `.env` was read only to confirm that git ignores it
  (all values masked).
- **Independent review:** one parent hunter, then two fresh verifier agents tried to
  disprove each candidate. All four confirmed findings survived, and the verifiers
  added one new finding (the scheme-less curl install). Deviations from the full skill:
  the verifiers worked in batches instead of one per candidate, and no
  `coverage-ledger.json` was written. Treat this as a first pass, not complete coverage.
- **Belongs to other sessions (reported here, not fixed):** the Airflow UI exposure
  (finding N1, now being moved behind `etl.jmdajmda.com` with a login), the CI/CD
  deploy (not on `main` yet, so not reviewed), and the spine id format change.
- **No prior runs.** This is run 1.

## 2. Security posture summary

The application code is in good shape. Every SQL value is parameterised, table names
come only from code constants, Airflow params are checked against `config/groups.json`,
no secrets appear in logs, and no real secret was ever committed. The risk is in **how
credentials and network trust are set up**. The remote SCADA devices keep the
**database-owner** password in a plaintext `.env` and connect over the internet
without verifying the server. The scrapers turn off TLS checks on the pages where they
type the vendor passwords.

## 3. Confirmed findings

| # | Severity | Title | Boundary | Result |
|---|---|---|---|---|
| F1 | **High** | Remote SCADA devices use the DB owner role. `spp_scada` does not exist | Remote device → whole warehouse | Whoever has a device's `.env` can DROP, TRUNCATE or UPDATE every table and read all data |
| F2 | Medium | Device → VPS Postgres does not authenticate the server | Internet path → DB session | An attacker on the path can relay the SCRAM login and keep an authenticated owner session |
| F3 | Medium | Chromium TLS checks are off while the scrapers type vendor passwords | Network → HMI credentials and readings | An attacker on the path can steal `BEATRICE_PWD` / `SC_PWD` and inject fake MW readings |
| F4 | Low | Astro CLI install runs a scheme-less `curl … \| sudo bash` | Network → root on the VPS | The first request goes out as plain HTTP, and the reply runs as root |

### F1 (High): remote devices log in as the database owner

- **Evidence:**
  - [DEPLOY_VPS.md:139-140](DEPLOY_VPS.md:139) creates a single role, `spp`, and makes it the owner of database `spp`.
  - [DEPLOY_VPS.md:662](DEPLOY_VPS.md:662) admits that role from the device: `hostssl spp spp <GARDEN_CITY_IP>/32`.
  - [DEPLOY_VPS.md:695](DEPLOY_VPS.md:695) puts the device DSN `postgresql://spp:<password>@…` in the device `.env`.
  - `schema/*.sql` and `schema/patches/*.sql` contain no `CREATE ROLE`, `GRANT`, `REVOKE` or `SECURITY DEFINER`.
  - `git grep -i spp_scada` over every ref, and `git log --all -S spp_scada`, find nothing. **The least-privilege `spp_scada` role was never built.**
- **What the devices actually need:** [garden_city.py:382-427](spp/scada/garden_city.py:382) and [sherburne_city.py:484-529](spp/scada/sherburne_city.py:484) do three things:
  1. `INSERT INTO timestamps (ts_central, ts_gmt) … ON CONFLICT DO NOTHING`.
  2. `SELECT ensure_month_partition('mw_updates', …)`, which runs `CREATE TABLE … PARTITION OF` ([01_schema.sql:257-275](schema/01_schema.sql:257)). Only the owner of the parent table may do that.
  3. `INSERT INTO mw_updates … ON CONFLICT DO NOTHING`.
- **Who can exploit it:** anyone who holds a device `.env` and can connect from the
  device's address. Examples: malware on the Windows device, a shared device account,
  or a copied backup.
- **Controls that limit it:** a `/32` pg_hba entry, ufw and the provider firewall
  restrict where connections can come from. `chmod 600` on `.env` does nothing on Windows.
- **Impact:** full owner rights on the warehouse: DROP, TRUNCATE or UPDATE any table,
  and read everything.
- **Fix:** run this as `spp` or `postgres`. No Python change is needed.

  ```sql
  CREATE ROLE spp_scada LOGIN PASSWORD '<new strong password>' NOINHERIT;
  REVOKE ALL ON DATABASE spp FROM PUBLIC;
  GRANT CONNECT ON DATABASE spp TO spp, spp_scada;
  GRANT USAGE ON SCHEMA public TO spp_scada;
  GRANT INSERT (ts_central, ts_gmt) ON timestamps TO spp_scada;
  GRANT INSERT ON mw_updates TO spp_scada;   -- parent only; routing needs no partition grant

  -- Partition creation runs with the owner's rights, and only for mw_updates:
  CREATE OR REPLACE FUNCTION ensure_month_partition(p_parent text, p_month date)
  RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
  DECLARE
      v_start date := date_trunc('month', p_month)::date;
      v_end   date := (date_trunc('month', p_month) + interval '1 month')::date;
      v_part  text := format('%s_y%sm%s', p_parent, to_char(v_start,'YYYY'), to_char(v_start,'MM'));
  BEGIN
      IF session_user <> 'spp' AND p_parent <> 'mw_updates' THEN
          RAISE EXCEPTION 'not allowed: %', p_parent;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = v_part) THEN
          EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                         v_part, p_parent, spine_id(v_start::timestamp), spine_id(v_end::timestamp));
      END IF;
  END $$;
  ALTER FUNCTION ensure_month_partition(text, date) OWNER TO spp;
  REVOKE EXECUTE ON FUNCTION ensure_month_partition(text, date) FROM PUBLIC;
  GRANT EXECUTE ON FUNCTION ensure_month_partition(text, date) TO spp, spp_scada;
  ```

  Then make three changes:
  - Change the pg_hba line to `hostssl spp spp_scada <DEVICE_IP>/32 scram-sha-256`.
  - Set every device DSN to `spp_scada`.
  - Change the `spp` password, because the old one is in device `.env` files.

  Keep the SQL as a new `schema/patches/` file, and add the same grants to
  `01_schema.sql`. Simpler alternative: pre-create the `mw_updates` partitions from
  `02_seed.sql` or an Airflow task, and give `spp_scada` no EXECUTE at all.
- **Optional follow-up:** give Airflow its own non-owner writer role too, so that `spp`
  is used only for migrations.

### F2 (Medium): device Postgres connection does not verify the server

- **Evidence:**
  - [DEPLOY_VPS.md:695](DEPLOY_VPS.md:695) uses `?sslmode=require` with no `sslrootcert`, no `verify-full` and no `channel_binding`.
  - [DEPLOY_VPS.md:674-676](DEPLOY_VPS.md:674) describes a self-signed certificate and gives no client-side verification steps.
- **Why it matters:**
  - `sslmode=require` encrypts the connection but accepts any certificate.
  - libpq's default `channel_binding=prefer` can be downgraded, so an attacker on the
    path can terminate TLS, relay the SCRAM-SHA-256 exchange to the real server, and
    keep the authenticated session. The `/32` rule does not help, because the relayed
    connection still arrives from the device's own path.
- **Impact:** an owner session today. Once F1 is fixed, insert-only.
- **Fix:**
  - Device `.env`, single-quoted: `SPP_DSN='postgresql://spp_scada:…@<host>:5432/spp?sslmode=verify-full&sslrootcert=C:/…/server.crt&channel_binding=require'`.
  - Give Postgres a certificate whose name matches `<host>`. Use a private CA, or keep
    the self-signed certificate and copy its `server.crt` to each device.
  - `channel_binding=require` alone already blocks the relay.

### F3 (Medium): TLS verification is off for the credentialed scrapers

- **Evidence:**
  - [beatrice.py:57](spp/scada/beatrice.py:57) sets `--ignore-certificate-errors`, then [beatrice.py:61-66](spp/scada/beatrice.py:61) types `BEATRICE_USER` and `BEATRICE_PWD`.
  - [sherburne_city.py:297-298](spp/scada/sherburne_city.py:297) set `--ignore-certificate-errors` and `--ignore-ssl-errors`, then [sherburne_city.py:375-379](spp/scada/sherburne_city.py:375) types `SC_USER` and `SC_PWD`.
  - [garden_city.py:259-261](spp/scada/garden_city.py:259) set the same flags. Garden City has no login, but its readings are trusted.
  - No code checks the URL scheme, so `http://` login URLs are also accepted.
- **Limits on injected readings:**
  - Values are checked only against a floor of 0 and a ceiling.
  - `BEATRICE_MW_CEIL` has no default, so Beatrice has no upper bound ([beatrice.py:88-90](spp/scada/beatrice.py:88)).
  - Sherburne defaults to a 100 MW ceiling. Garden City defaults to 10 and 20.
- **Impact:** an attacker on the path between the VPS or device and the vendor HMI can
  steal the vendor logins and write fake MW readings to `mw_updates`.
- **Fix:**
  - Remove the ignore flags.
  - If an HMI uses a self-signed certificate, pin it with
    `--ignore-certificate-errors-spki-list=<base64 SPKI sha256>` (needs Chrome 66 or later)
    instead of turning off all checks.
  - Refuse login URLs that are not `https://` before `driver.get`.
  - Give `BEATRICE_MW_CEIL` a default.

### F4 (Low): scheme-less installer piped to `sudo bash`

- **Evidence:** [DEPLOY_VPS.md:111](DEPLOY_VPS.md:111) runs `curl -sSL install.astronomer.io | sudo bash -s`. With no scheme, curl uses `http://`.
- **Fix:** `curl --proto '=https' --tlsv1.2 -sSfL https://install.astronomer.io -o astro-install.sh`, read the script, then run `sudo bash astro-install.sh`.

## 4. Needs validation (no severity until checked)

| # | Lead | Trace | Open question | What to check on the VPS (owner) |
|---|---|---|---|---|
| N1 | Docker-published Astro ports bypass ufw: an unauthenticated Airflow UI and the metadata Postgres may be reachable from the internet | [DEPLOY_VPS.md:206-217](DEPLOY_VPS.md:206). [docker-compose.override.yml](docker-compose.override.yml) has no loopback `ports:` bindings | Is the DigitalOcean cloud firewall attached, and what is listening on `0.0.0.0`? | `sudo ss -tlnp` (look for 5435, 8080, 6563 and the api-server port), and check the firewall in the DO console. Severity would be **high** if they are exposed: running DAGs, reading Variables and connections, reaching the metadata DB. Fix: set `{"ip": "127.0.0.1"}` in `/etc/docker/daemon.json`. *(The etl-subdomain session owns this area.)* |
| N2 | DB tests delete rows in whatever `SPP_TEST_DSN` `.env` sets, and the local value points to a Neon database named `neondb` with role `neondb_owner` | [spp/config.py:23](spp/config.py:23) `load_dotenv()`. [tests/test_ingest_e2e.py:34](tests/test_ingest_e2e.py:34) runs `DELETE FROM rt_lmp WHERE interval_central = …`, which hits every node for that interval | Does the Neon database hold data that matters? | Confirm Neon is disposable. Fix: in each DB test module, refuse to run unless the dbname ends in `_test`. The other test deletes are limited to `TEST_%` rows. *(The CI/CD session noted the same issue.)* |

## 5. Hardening notes (not vulnerabilities)

- **Chromium `--no-sandbox` in the scheduler container.**
  - Where: [beatrice.py:56](spp/scada/beatrice.py:56) sets it always; [sherburne_city.py:317-323](spp/scada/sherburne_city.py:317) sets it when `SCADA_CHROME_BINARY` is set.
  - Why it matters: that container's environment holds `SPP_DSN` and every site login, and `packages.txt` installs an unpinned Chromium that is updated only when the image is rebuilt. A Chromium renderer exploit would reach all of those secrets, and F3 makes such an exploit easier to deliver.
  - What to do: rebuild the image regularly, and run the scrapers in a container that does not hold the warehouse DSN.
- **Dependencies.**
  - `requirements.txt` uses open-ended `>=` for psycopg2-binary, pytest, sqlglot, python-dotenv and selenium, with no lock or hash file.
  - `pytest` and `sqlglot` ship in the production image.
  - The Dockerfile pins the base image by tag, not digest.
  - What to do: use `pip-compile --generate-hashes`, move dev dependencies to a separate file, and pin `runtime:3.3-7@sha256:…`.
- **Plaintext `.env` on Windows devices.** Restrict it to the service account with
  `icacls .env /inheritance:r /grant:r "<svc-user>:R"`, and run the scraper under a
  dedicated non-admin account.
- **Backups.** [DEPLOY_VPS.md:758-781](DEPLOY_VPS.md:758) writes unencrypted `pg_dump`
  files to the same VPS. Encrypt them (`age` or `gpg`) before copying them off-host.
- **launchd logs.** The plist writes logs to fixed paths in `/tmp`
  ([plist:44,46](deploy/launchd/com.valuehash.scada-garden-city.plist:44)). Use
  `~/Library/Logs` instead.
- **Docker pg_hba range.** `host spp spp 172.16.0.0/12` ([DEPLOY_VPS.md:161](DEPLOY_VPS.md:161))
  admits every Docker network on the host. Narrow it to the Astro project's subnet.

## 6. Good patterns found

- **Secrets in git:** `.env` is git-ignored (`.gitignore:2`) and was never committed;
  only `.env.example` with placeholders was. The local `.env` is mode 600. A scan of
  `git log --all -p` found no real passwords, DSNs, keys or Neon hosts.
- **SQL:** every value is bound with `%s` / `%(x)s`. Identifiers in
  [spp/db.py:187-225](spp/db.py:187), `spp/fill.py` and the DAGs come only from the
  `FEEDS` constants. `ensure_month_partition` uses `%I` / `%L`.
- **Airflow input:** the `groups` trigger param is checked against
  `config/groups.json` (`spp/selection.py:76-92`). Members are used only as a pandas
  filter.
- **Logs:** no log line includes `SPP_DSN` or a password. The `LoginFailed` messages
  name the variables, not their values.
- **Shell script:** every expansion in `scripts/backfill.sh` is quoted, and extra
  arguments are passed as an array.
- **Deployment:** a read-only, per-host deploy key, host-key fingerprint checking,
  `pull.ff only`, a non-root systemd user unit, and `hostssl` + `scram-sha-256` for the
  device.

## 7. Coverage

| Area | Status |
|---|---|
| DAGs, loaders, SQL composition, Airflow params, logging | Covered. Nothing found |
| SCADA scrapers (Beatrice, Garden City, Sherburne) | Covered → F3 |
| SQL roles and privileges | Covered → F1 |
| VPS deployment (pg_hba, ufw, Docker, TLS, backups) | Covered → F1, F2, F4, N1 |
| Dependencies and image | Covered → hardening notes |
| Git history, secrets | Covered. Clean |
| Tests | Covered → N2 |
| CI/CD workflow, Airflow UI auth proxy | Out of scope: not on `main`; other sessions own them |
| Live configuration (VPS, DO firewall, devices, vendor HMI certificates) | Not observable from source |

## Fix these first

1. **F1:** create `spp_scada`, switch the devices to it, and **change the `spp` password**.
2. **N1:** run `sudo ss -tlnp` and check the DO firewall today. If any Astro port is
   public, set `{"ip":"127.0.0.1"}` in `daemon.json`.
3. **F2:** add `channel_binding=require` (and `verify-full` with `sslrootcert`) to every
   device DSN.
4. **F3:** remove `--ignore-certificate-errors`; pin self-signed HMI certificates if
   needed; set a default `BEATRICE_MW_CEIL`.
5. **N2:** add a `_test` dbname guard to the DB tests before anyone runs `pytest` with
   the current `.env`.
