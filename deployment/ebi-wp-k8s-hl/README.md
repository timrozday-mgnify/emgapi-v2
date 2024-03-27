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
`task exec -- migrate`

# Auth (needed first, if this is a from-scratch setup)
Auth is handled using an OAuth2-proxy in front of Prefect.
Unauthenticated users will be redirected to an auth screen.
Currently using GitHub org as the auth backend, since EBI LDAP not available in this deployment.

## Create a `secrets-github-auth.env` file:
```bash
OAUTH2_PROXY_CLIENT_ID=...
OAUTH2_PROXY_CLIENT_SECRET=...
```

These should be creds for a github oauth2 application, which will handle authentication for users.

`kubectl create secret generic github-oauth-secret --from-env-file=secrets-github-oauth.env -n emgapiv2-hl-exp`

## Create other secrets:
Trusted IPs of worker nodes, that should not need to authenticate to access the Prefect API.
`kubectl create secret generic oauth2-proxy-trusted-ip --from-literal=oauth2-proxy-trusted-ip='1.1.1.1' -n emgapiv2-hl-exp
secret/oauth2-proxy-trusted-ip created`

Cookie secret:
`kubectl create secret generic oauth2-proxy-cookie-secret --from-literal=oauth2-proxy-cookie-secret='<some base64 thing>' -n emgapiv2-hl-exp`
