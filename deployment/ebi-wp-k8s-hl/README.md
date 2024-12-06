# Deploying to WebProd Kubernetes at EBI

## Regular redeployments:
Build the specific dockerfile for WP Kubernetes, push it to quay, and restart the app on WP K8s:
`task ebi-wp-k8s-hl:update-api`

To deploy a newly written flow, for example:
`FLOW=assemble_study task ebi-wp-k8s-hl:deploy-flow`
This *ONLY* deploys the flow to the prefect server. It doesn't mean the workers yet have access to that code.

If any code related to prefect flows has changed, or if the DB schema has changed, you'll also need to restart any prefect workers.
Look for a `deploy-prefect-worker` job on the Microbiome Informatics Jenkins, to do so.

## Initial deployment:

### Create a `secrets-k8s.env` file
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

### Create Quay.io pull secrets
* Get authentication credentials for quay.io (the built image is private). You can get a Kubernetes secrets yaml file from your Quay.io user settings, in the "CLI Password" section.
* Download the secrets yaml and name the secret `name: quay-pull-secret` in the metadata section. Give it the right namespace. Put it into this folder.
* `kubectl apply -f secrets-quayio.yml`

### Deploy
`kubectl apply -f ebi-wp-k8s-hl.yaml`

### Migrate
`task migrate`

# Auth (needed first, if this is a from-scratch setup)
Auth is handled using an OAuth2-proxy in front of Prefect.
Unauthenticated users will be redirected to an auth screen.
Currently, we're using GitHub org as the auth backend since EBI LDAP not available in this deployment.

## Create a `secrets-github-auth.env` file:
```bash
OAUTH2_PROXY_CLIENT_ID=...
OAUTH2_PROXY_CLIENT_SECRET=...
```

These should be creds for a github oauth2 application, which will handle authentication for users.

`kubectl create secret generic github-oauth-secret --from-env-file=secrets-github-oauth.env -n emgapiv2-hl-exp`

## Worker's auth
Prefect workers also need to be able to authenticate to the Prefect Server API.
This isn't built into Prefect, and they can't use oauth2 easily.

In this deployment we are using [MITMProxy in upstream mode](https://docs.mitmproxy.org/stable/concepts-modes/#upstream-proxy),
on the workers, to intercept HTTP requests to the Prefect API and add a basic auth header.
The credentials for this need to be added to Kubernetes, as well as env vars on the worker VM.
E.g. if the worker is deployed by a Jenkins job, these basic auth headers can be in Jenkins secrets and MITM Proxy should be started before the prefer worker.

```shell
 htpasswd -c auth my-worker-name...
 #<enter a password twice>
kubectl create secret generic prefect-workers-basic-auth --from-file=auth -n emgapiv2-hl-exp
```

And on the worker, before starting prefect (to also chain the EBI Proxy):
```shell
export PREFECT_API_AUTH_PASSWORD=my-worker-name...
export PREFECT_API_AUTH_USERNAME=mypassword...
export PREFECT_API_URL='http://prefect-dev.mgnify.org/workers/api'
mitmdump --mode upstream:http://hh-wwwcache.ebi.ac.uk:3128 -s mitm_auth_for_workers.py
```

## Create other secrets:
Cookie secret:
`kubectl create secret generic oauth2-proxy-cookie-secret --from-literal=oauth2-proxy-cookie-secret='<some base64 thing>' -n emgapiv2-hl-exp`

# Auth setup:

```text
┌────────────────────────────────────────┬───────┬───────────────────────────────────────────────┐  ┌──────────────────────────────────────┬────────────┬──────────────────────────────────┐
│                                        │ CODON │                                               │  │                                      │ KUBERNETES │                                  │
│                                        └───────┘                                               │  │                                      └────────────┘                                  │
│                                                                                                │  │                                                                                      │
│                                                                                                │  │                                                                                      │
│          ┌───────────────────────┬─────────────────┬────────────────────────┐                  │  │                                           ┌─────────┬────────────────┬──────────┐    │
│          │                       │ mi-slurm-worker │                        │                  │  │                                           │         │ prefect-server │          │    │
│          │                       └─────────────────┘                        │                  │  │                                           │         └────────────────┘          │    │
│          │                                                                  │                  │  │        worker ingress                     │     API             UI              │    │
│          │   ┌───┬────────────────┬───┐    ┌────┬────────────┬─────┐        │                  │  │      ┌──────────────────┐                 │  ┌───────────┐  ┌──────────┐        │    │
│          │   │   │ prefect worker │   │    │    │ mitm-proxy │     │        │                  │  │      │                  │                 │  │           │  │          │        │    │
│          │   │   └────────────────┘   │    │    └────────────┘     │        │                  │  │      │                  │                 │  │           │  │          │        │    │
│          │   │                        │    │                       │        │     polls for work  │      │check auth header │                 │  │           │  │          │        │    │
│          │   │ prefect worker start   ├───►│ +http auth headers    ├────────┼──────────────────┬──┼──────┼──────────────────┼─────────────────┼─►│           │◄─┤          │        │    │
│          │   │                        │    │                       │        │                  │  │      │                  │                 │  │           │  │          │        │    │
│          │   │                        │    └───────────────────────┘        │                  │  │      └──────────────────┘                 │  │           │  │          │        │    │
│          │   └───────────────┬────────┘                                     │                  │  │                                           │  │           │  │          │        │    │
│          │                   │                                              │                  │  │                                           │  └───────────┘  └──────────┘        │    │
│          └───────────────────┼──────────────────────────────────────────────┘                  │  │                                           │                       ▲             │    │
│                              │                                                                 │  │         oauth ingress                     │                       │             │    │
│                              │ submits to                                                      │  │      ┌──────────────────┐                 │                 ┌─────┼────┐        │    │
│                              │                                                                 │  │      │                  │                 │     oauth2-proxy│     │    │        │    │
│                 slurm nodes  ▼                                                                 │  │      │                  │  logged in      │                 │     │    │        │    │
│               ┌──────────────┐                                                                 │  │      │         ┌────┬───┴─────────────────┼─────────────────┼─────┘    │        │    │
│               │              │                                                                 │  │      │         │    │   ◄─────────────────┼─────────────────┤          │        │    │
│               │  ┌───────────┼───┐                                                             │  │      └─────────┼────┼───┘                 │                 └──────────┘        │    │
│               │  │           │   │                                                             │  │                │    │                     │                                     │    │
│               │  │  ┌────────┼───┼───┐                                                         │  │                │    │                     └─────────────────────────────────────┘    │
│               │  │  │        │   │   │                                                         │  │                │    │login needed                                                    │
│               │  │  │        │   │   │                                                         │  │                │    │                                                                │
│               └──┼──┼────────┘   │   │                                                         │  └────────────────┼────┼────────────────────────────────────────────────────────────────┘
│                  │  │            │   │                                                         │                   │    │       github oauth provider
│                  └──┼────────────┘   │                                                         │                   │    │        ┌─────────────────┐
│                     │                │                                                         │                        │        │                 │
│                     └────────────────┘                                                         │                  user  └───────►│      tokens     │
│                                                                                                │                                 │                 │
└────────────────────────────────────────────────────────────────────────────────────────────────┘                 ┌───┐           └─────────────────┘
                                                                                                                   │###│
                                                                                                                   ├───┤
                                                                                                                  ┌┴───┴┐
                                                                                                                  │ ### │
                                                                                                                  │ ### │
                                                                                                                    │ │
                                                                                                                    │ │
```
