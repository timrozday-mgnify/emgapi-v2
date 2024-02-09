# Deploying to WebProd Kubernetes at EBI

## Create a `secrets-k8s.env` file
```bash
DJANGO_SECRET_KEY=...
DATABASE_URL=postgres://...:...@app-database:5432/...
POSTGRES_APP_USER=...
POSTGRES_APP_PASSWORD=...
POSTGRES_PREFECT_USER=...
POSTGRES_PREFECT_PASSWORD=...
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://...:...@postgres-prefect:5432/...
```

Make the secrets
`kubectl create secret generic emgapi-secret --from-env-file=secrets-k8s.env -n emgapiv2-hl-exp`

## Create Quay.io pull secrets
* Get authentication credentials for quay.io (the built image is private). You can get a Kubernetes secrets yaml file from your Quay.io user settings, in the "CLI Password" section.
* Download the secrets yaml and name the secret `name: quay-pull-secret` in the metadata section. Give it the right namespace. Put it into this folder.
* `kubectl apply -f secrets-quayio.yml`

## Deploy
`kubectl apply -f ebi-wp-k8s-hl.yaml`

## Migrate
`kubectl get nodes -n emgapiv2-hl-exp`

Find the emgapi pod.

`kubectl exec -it <the pod name> -- /bin/sh`
`python manage.py migrate`

##
