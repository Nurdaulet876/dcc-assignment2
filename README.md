# Replicated Counter Service

## Setup
python -m venv .venv

source .venv/bin/activate          # Windows: .venv\\Scripts\\activate 

pip install grpcio grpcio-tools pytest

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto

## Start one replica
python server.py --port 50051 --name replica-A

python client.py --name client-1 incr likes:post-42 --by 1

python client.py --name client-1 get likes:post-42


## Start three replicas (three terminals)
python server.py --port 50051 --name replica-A

python server.py --port 50052 --name replica-B

python server.py --port 50053 --name replica-C

python client.py --addrs localhost:50051,localhost:50052,localhost:50053 incr likes:post-42 --by 1


## Run the test suite

pytest -v


## Run the benchmark

python tests/perf_benchmark.py --runs 3


## Fault flags (server)

`--delay-ms N` (slow replica), `--delay-first K` (delay only first K requests), `--quiet`.


## Run a cluster by hand (leaseholder = A on 50051)

python server.py --name A &      # port 50051, leaseholder

python server.py --name B &      # port 50052

python server.py --name C &      # port 50053

python client.py --addr localhost:50051 put user:1 alice                  # QUORUM_CP

python client.py --addr localhost:50053 put user:1 bob --strategy ap      # AP, any node

python client.py --addr localhost:50052 get user:1 --consistency lin      # proxied to A

python client.py --addr localhost:50052 get user:1 --consistency stale --max-staleness-ms 5000

python client.py --addr localhost:50052 sync                              # anti-entropy now


## Tests and benchmark

python -m pytest tests/test_cap.py -v --junitxml=results/pytest_junit.xml

python perf_cap_bench.py --runs 3          
