# Pipeline CI/CD

## Vue d'ensemble

Ce projet utilise GitHub Actions pour l'intégration continue et le déploiement continu.

| Étape | Workflow | Déclencheur | Runner |
|-------|----------|-------------|--------|
| CI    | `.github/workflows/validate.yml` | push / PR sur `main` | `ubuntu-latest` |
| CD    | `.github/workflows/deploy.yml`   | push sur `prod` / manuel | `self-hosted` (VM de production) |

---

## CI — Validation (`validate.yml`)

S'exécute à chaque push et pull request ciblant `main`.

Étapes :

1. Checkout du dépôt
2. Installation de Python 3.12 via `uv`
3. Installation des dépendances CI (`uv sync --extra ci`)
4. Validation des fichiers de flows Kestra via `check_flows.py`
5. Exécution de la suite de tests avec `pytest`

Le pipeline CI garantit que :

- tous les fichiers YAML de flows sont correctement parsés
- les champs requis (`id`, `namespace`, `tasks`) sont présents
- aucun ID de flow n'est dupliqué
- aucun identifiant en dur n'apparaît dans les définitions de flows
- tous les tests Python passent

Cette étape doit réussir avant toute fusion dans `main`.

---

## CD — Déploiement (`deploy.yml`)

### Déclencheurs

- **Push sur `prod`** — déploiement automatique à chaque merge ou push.
- **Dispatch manuel** — déclenché via l'interface GitHub Actions (`workflow_dispatch`).

Le déploiement n'est **pas** déclenché sur `main`.

### Runner

Le workflow s'exécute sur un **runner GitHub Actions self-hosted** installé sur la VM de production. Cela permet au workflow d'appeler l'API Kestra en `localhost` sans l'exposer sur Internet.

### Séquence de déploiement

1. GitHub détecte un push sur `prod` (ou un déclenchement manuel)
2. Le job `deploy` démarre sur le runner self-hosted
3. Le dépôt est cloné (`actions/checkout@v4`)
4. La branche, le commit et le hostname sont loggés pour traçabilité
5. `100-scripts_mlops/deploy/deploy_flows.sh` est exécuté
6. Le script valide chaque flow via l'API Kestra, puis le met à jour

Si une étape échoue, le workflow s'arrête immédiatement.

---

## Script de déploiement (`deploy_flows.sh`)

Situé dans `100-scripts_mlops/deploy/deploy_flows.sh`.

Le script itère sur tous les fichiers `.yaml` / `.yml` du répertoire de flows et, pour chaque flow :

1. **Valide** le flow via `POST /api/v1/main/flows/validate`
2. **Déploie** le flow via `PUT /api/v1/main/flows/{namespace}/{id}`

### Variables d'environnement

| Variable | Description | Défaut |
|----------|-------------|--------|
| `KESTRA_URL` | URL de base de l'API Kestra | `http://localhost:8082` |
| `FLOW_DIR` | Répertoire contenant les fichiers YAML des flows | `10-flows` |
| `KESTRA_ADMIN_USER` | Nom d'utilisateur de l'API Kestra | _(requis)_ |
| `KESTRA_ADMIN_PASS` | Mot de passe de l'API Kestra | _(requis)_ |

Les identifiants sont fournis via les secrets du dépôt GitHub — ils ne sont jamais codés en dur.

Le script utilise `set -euo pipefail` : toute erreur (validation échouée, problème réseau, mauvaise réponse) provoque un arrêt immédiat.

---

## Secrets

Les secrets suivants doivent être configurés dans les paramètres du dépôt GitHub :

| Secret | Utilisé par |
|--------|-------------|
| `KESTRA_ADMIN_USER` | `deploy.yml` → `deploy_flows.sh` |
| `KESTRA_ADMIN_PASS` | `deploy.yml` → `deploy_flows.sh` |

---

## Workflow de développement

```
branche feature ──► PR vers main ──► CI valide ──► merge dans main ──► merge main → prod ──► CD déploie
```

1. Créer une branche feature depuis `main`
2. Développer et tester en local (`pytest 130-tests/ -v`)
3. Ouvrir une pull request vers `main`
4. La CI exécute la validation — tous les checks doivent passer
5. Merge dans `main`
6. Quand prêt pour la production, merge de `main` dans `prod`
7. Le CD déploie automatiquement les flows mis à jour sur l'instance Kestra de production
