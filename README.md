# srunner
Spanner Runner

## test

```
gcloud emulators spanner start --host-port localhost:9050
export SPANNER_EMULATOR_HOST="localhost:9050" 

go test ./...
```