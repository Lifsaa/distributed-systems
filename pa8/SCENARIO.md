# PA8 Recording Script

Use two terminals.

Terminal 1 watches the cluster:

```bash
cd /Users/lifsa/Desktop/distributed-systems/pa8
make logs
```

Terminal 2 runs the scenario commands:

```bash
cd /Users/lifsa/Desktop/distributed-systems/pa8
```

Replica numbers are examples. The real rule is to stop followers when the step
says follower, and stop the current leader when the step says leader. If a client
command prints `not leader`, rerun the same command with the shown `LeaderAddr`.

## Before Each Scenario

```bash
make reset-scenario
make up
```

Wait until Terminal 1 shows a line like:

```text
CANDIDATE SERVER 3 WINS THE ELECTION FOR TERM 2
```

Optional quick check:

```bash
make seed
```

If `make seed` says `not leader`, use the printed leader address for the load
commands.

## Scenario 1

Purpose: show follower failure, leader failure, recovery, and final log match.

1. Stop one follower. Example:

```bash
docker compose stop replica5
```

2. Send 20 requests to the current leader. Example:

```bash
make load20 LEADER=localhost:5001
```

3. Stop the current leader. Example:

```bash
docker compose stop replica1
```

4. Wait for a new leader, then send 30 requests to it. Example:

```bash
make load30 LEADER=localhost:5002
```

5. Stop that current leader. Example:

```bash
docker compose stop replica2
```

6. Restart the replicas that were stopped earlier. Example:

```bash
docker compose start replica5 replica1
```

7. Wait for a leader, send 40 requests, then verify and save:

```bash
make load40 LEADER=localhost:5003
make test-logs
make save-scenario NAME=scenario1
```

Expected final line:

```text
PASS logs match
```

## Scenario 2

Purpose: show that two live servers cannot elect a leader, then recovery after
majority comes back.

1. Reset and start fresh:

```bash
make reset-scenario
make up
```

2. Stop two followers. Example:

```bash
docker compose stop replica4 replica5
```

3. Send 20 requests to the current leader. Example:

```bash
make load20 LEADER=localhost:5001
```

4. Stop the current leader. Now only two servers are up, so elections should not
produce a working leader. Example:

```bash
docker compose stop replica1
```

5. Restart one stopped follower so the cluster has a majority again:

```bash
docker compose start replica4
```

6. Wait for a new leader, then send 30 requests. Example:

```bash
make load30 LEADER=localhost:5002
```

7. Stop the current leader. Example:

```bash
docker compose stop replica2
```

8. Restart one previous server so another leader can be elected:

```bash
docker compose start replica1
```

9. Wait for a leader, then send 40 requests. Example:

```bash
make load40 LEADER=localhost:5001
```

10. Restart the final stopped follower, verify, and save:

```bash
docker compose start replica5
make test-logs
make save-scenario NAME=scenario2
```

Expected final line:

```text
PASS logs match
```

## Saved Evidence

Each `make save-scenario NAME=scenarioX` creates:

```text
recordings/scenarioX/server-logs/
recordings/scenarioX/docker-compose.log
recordings/scenarioX/docker-compose-ps.txt
recordings/scenarioX/tester-output.log
```

Save both scenarios before making the final submission zip.
