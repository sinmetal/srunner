# srunner
Spanner Runner

## test

```
gcloud emulators spanner start --host-port localhost:9050
export SPANNER_EMULATOR_HOST="localhost:9050" 

go test ./...
```

## k8s

```
gcloud config set PROJECT_ID
```

```
kubectl apply -f k8s/metalapps-namespace.yaml
```

```
kubectl create serviceaccount metalapps-default \
    --namespace metalapps
```

```
gcloud iam service-accounts add-iam-policy-binding metalapps@GSA_PROJECT.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:PROJECT_ID.svc.id.goog[metalapps/metalapps-default]"
```

```
kubectl annotate serviceaccount metalapps-default \
    --namespace metalapps \
    iam.gke.io/gcp-service-account=metalapps@GSA_PROJECT.iam.gserviceaccount.com
```