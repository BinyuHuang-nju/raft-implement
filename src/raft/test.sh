

for i in $(seq 1 1 15)
do
	go test -run Election
	go test -run BasicAgree
	go test -run FailAgree
	go test -run FailNoAgree
	go test -run ConcurrentStarts
	go test -run Rejoin
	go test -run Backup
	go test -run Count
	go test -run Persist
	go test -run Figure8
	go test -run UnreliableAgree
	go test -run ReliableChurn
	go test -run UnreliableChurn
done
