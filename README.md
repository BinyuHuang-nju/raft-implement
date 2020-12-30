# raft-implement

## Note
	-This is BinyuHuang's raft implement code. You can find it in /src/raft/raft.go.
	- There exists some bugs which make testcase Persist2 and Figure8Unreliable have a certain chance to fail.
	- The work needs to be modified afterwards.
	- I provide a simple script to run test automatically and you can find it in /src/raft/test.sh.

## Build & Run
	-If you use some editor or IDE supporting Go, edit the configure by setting GOPATH=$(root)

	-With terminal, enter the root directory.
	 $ export GOPATH=$PWD
	 $ cd /src/raft
	 $ go test -run Election/BasicAgree/.... or sh test.sh

## Reference
	-This is the resource repository for the course Distributed System, Fall 2017, CS@NJU.

	-In Assignment 2 and Assignment 3, you should primarily focus on /src/raft/...

