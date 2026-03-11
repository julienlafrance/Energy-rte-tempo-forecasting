# Déploiement continu — Flows Kestra

## Déclencheurs

Le déploiement s'exécute dans deux cas :

- **Push sur `prod`** — déploiement automatique à chaque merge/push sur la branche de production.
- **Dispatch manuel** — déclenché via l'interface GitHub Actions (`workflow_dispatch`).

Le déploiement n'est **pas** déclenché sur `main`.

## Runner

Le workflow s'exécute sur un **runner GitHub Actions self-hosted** installé sur la VM de production. Kestra est accessible localement à `http://localhost:8082`.

## Fonctionnement

1. Clone le dépôt.
2. Logge la branche, le SHA du commit et le hostname du runner pour traçabilité.
3. Exécute `100-scripts_mlops/deploy/deploy_flows.sh`, qui valide puis met à jour les flows Kestra via l'API.

## Secrets

Les secrets suivants doivent être configurés dans le dépôt GitHub :

| Secret               | Description                        |
|----------------------|------------------------------------|
| `KESTRA_ADMIN_USER`  | Nom d'utilisateur admin Kestra     |
| `KESTRA_ADMIN_PASS`  | Mot de passe admin Kestra          |
