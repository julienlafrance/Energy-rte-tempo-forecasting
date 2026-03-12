# Pipeline CI/CD — Kestra Flows

## Vue d'ensemble

| Workflow | Fichier | Déclencheur | Runner |
|----------|---------|-------------|--------|
| **CI** | `.github/workflows/ci.yml` | PR / push vers `main` | `ubuntu-latest` + `[self-hosted, dev]` |
| **Protect prod** | `.github/workflows/protect-prod.yml` | PR vers `prod` | `ubuntu-latest` |
| **CD** | `.github/workflows/deploy.yml` | push vers `prod` + `workflow_dispatch` | `[self-hosted, prod]` |

Namespace cible : **`projet713`**

### Branches

| Branche | Rôle | Protégée |
|---------|------|----------|
| `main` | Intégration — code validé par la CI, prêt à être promu | ✅ |
| `prod` | Production — seul le code promu ici est déployé | ✅ |

> **Un merge dans `main` ne déclenche pas de déploiement.** Le déploiement prod nécessite une promotion explicite vers `prod`.

---

## CI — `.github/workflows/ci.yml`

S'exécute sur chaque pull request et push vers `main`.

### Job 1 : `lint-and-test` (ubuntu-latest)

Checks statiques, sans serveur Kestra :

1. **yamllint** — syntaxe YAML, indentation, longueur de ligne sur `10-flows/prod/`
2. **SQL lint** — vérifie que les fichiers dans `140-sql/queries/` sont lisibles
3. **Flow scripts** — vérifie la syntaxe Python (`py_compile`) des scripts dans `100-scripts_mlops/` (hors `ci/` et `deploy/`)
4. **check_flows.py** — validation custom :
   - champs requis (`id`, `namespace`, `tasks`)
   - namespace = `projet713`
   - pas de flow IDs dupliqués
   - références subflow résolues
   - références `read('...')` pointent vers des fichiers SQL existants
   - clés `kv('...')` dans l'ensemble connu (chargé depuis `kestra_kv_keys.yaml`)
   - pas de secrets hardcodés
5. **pytest** — tests unitaires et intégration (`130-tests/`)

### Job 2 : `validate-kestra` (`[self-hosted, dev]`)

Validation serveur via `kestra-io/validate-action` :

- S'exécute **après** `lint-and-test`
- Valide les flows contre l'API Kestra DEV (`http://localhost:8082` sur VM 705)
- Utilise le runner `[self-hosted, dev]` — la CI ne touche jamais au serveur prod

---

## CD — `.github/workflows/deploy.yml`

Déploie sur Kestra prod quand du code est **promu sur la branche `prod`**.

### Déclenchement

- `push` vers `prod` avec filtre `paths` sur `10-flows/prod/**`, `140-sql/queries/**` et `100-scripts_mlops/**` (hors `ci/`, `deploy/`)
- `workflow_dispatch` pour déclenchement manuel

### Runner

Runner `[self-hosted, prod]` sur la VM de production (713). Kestra est accessible en `http://localhost:30082`.

### Séquence

1. **Checkout** avec `fetch-depth: 2` (pour permettre le rollback)
2. **Contexte** — affiche branche, commit, hostname, date, namespace, serveur
3. **Deploy SQL** — namespace files via `kestra-io/deploy-action`
4. **Validate flows** — via `kestra-io/validate-action`
5. **Deploy flows** — via `kestra-io/deploy-action` (`delete: false`)
6. **Smoke tests** :
   - les 5 flows existent via API
   - tous les namespace files SQL versionnés dans Git existent
   - les clés KV critiques (`PG_JDBC`, `MQTT_SERVER`, `MLFLOW_TRACKING_URI`) sont accessibles
7. **Notification Discord** (succès)
8. **Rollback** en cas d'échec — redéploie les fichiers du commit précédent
9. **Notification Discord** (échec + rollback)

### Ordre important

Le SQL (namespace files) doit être déployé **avant** les flows, car `mqtt_linky_gold` fait `read('queries/linky_gold.sql')`.

### Garanties de déploiement non destructif

Le pipeline CD est conçu pour être **conservatif** :

- **Flows** : `delete: false` garantit que les flows déjà présents sur Kestra mais absents du repo Git ne sont **pas supprimés**. Seuls les flows versionnés dans Git sont créés ou mis à jour.
- **Namespace files (SQL)** : `kestra-io/deploy-action` avec `resource: namespace_file` crée ou met à jour les fichiers présents dans Git. Les namespace files déjà sur Kestra mais absents du dépôt ne sont **pas affectés**.
- **Flow scripts (Python)** : les scripts dans `100-scripts_mlops/` (hors `ci/` et `deploy/`) sont vérifiés en CI et restaurés en rollback. Ils s'exécutent directement sur la VM via les volumes montés dans Kestra — aucun déploiement API n'est nécessaire, le `git checkout` suffit.
- **Rollback** : en cas d'échec, le script `rollback_prod.sh` utilise `git ls-tree` + `git show` pour énumérer et restaurer **tous** les fichiers qui existaient à `HEAD~1`, y compris les fichiers **supprimés** entre les deux commits (ce que `git checkout HEAD~1 -- <dir>` ne gère pas). Seules les ressources versionnées dans Git (flows + SQL + scripts) sont re-déployées. Le rollback ne supprime rien.

> **Principe** : Git est la source de vérité pour les ressources qu'il gère, mais le déploiement ne touche jamais aux ressources non gérées par Git.

---

## Secrets GitHub requis

| Secret | Usage | Obligatoire |
|--------|-------|-------------|
| `KESTRA_ADMIN_USER` | Authentification API Kestra (CI + CD) | ✅ |
| `KESTRA_ADMIN_PASS` | Authentification API Kestra (CI + CD) | ✅ |
| `DISCORD_WEBHOOK_URL` | Notifications Discord (CD) | Recommandé |

---

## Artifacts versionnés du pipeline

Le pipeline CI/CD gère trois types d'artifacts :

| Type | Répertoire | Déployé via | Rollback |
|------|-----------|-------------|----------|
| **Flows Kestra** | `10-flows/prod/` | `kestra-io/deploy-action` (API) | `git ls-tree` + API PUT par flow |
| **Namespace files SQL** | `140-sql/queries/` | `kestra-io/deploy-action` (API) | `git ls-tree` + API POST par fichier |
| **Flow scripts Python** | `100-scripts_mlops/` | `git checkout` (sur la VM) | `git ls-tree` + `git show HEAD~1` |

**Exclusions** : les répertoires `100-scripts_mlops/ci/` et `100-scripts_mlops/deploy/` contiennent du code interne au CI/CD. Ils ne sont jamais inclus dans la validation des scripts flows, le rollback, ni le déploiement.

---

## Configuration des clés KV

Les clés Kestra KV attendues sont définies dans **`kestra_kv_keys.yaml`** à la racine du repo (single source of truth). `check_flows.py` charge ce fichier en CI pour vérifier que les `kv('...')` référencés dans les flows correspondent à des clés connues.

Pour ajouter une nouvelle clé KV : l'ajouter dans `kestra_kv_keys.yaml` et commiter.

---

## Script legacy : `deploy_flows.sh`

Le script `100-scripts_mlops/deploy/deploy_flows.sh` est conservé comme outil de **débogage manuel / fallback d'urgence** uniquement. Il n'est plus utilisé dans les workflows CI/CD.

---

## Runners

| Runner | Machine | Usage |
|--------|---------|-------|
| `ubuntu-latest` | GitHub-hosted | Checks statiques CI (lint, tests) |
| `[self-hosted, dev]` | VM dev 705 | Validation Kestra CI |
| `[self-hosted, prod]` | VM prod 713 | Déploiement CD |

---

## Flux de travail recommandé

```
feature → PR vers main → CI → merge → PR main→prod → contrôle + approbation → merge → CD prod
```

1. Créer une branche feature : `git checkout -b feature/mon-changement`
2. Modifier les flows, SQL ou scripts
3. Commiter, pousser, ouvrir une PR vers `main`
4. La CI s'exécute automatiquement (yamllint, check_flows, pytest, validate-action sur DEV)
5. Si la CI passe → merge dans `main`
6. Quand le code est prêt pour la prod, ouvrir une **PR de `main` vers `prod`** via GitHub
7. Le workflow `protect-prod.yml` vérifie que la source est bien `main`
8. Un pair approuve la PR
9. Merge la PR → le push sur `prod` déclenche le CD
10. Smoke tests + notification Discord

> **Ne jamais** push directement sur `prod`. Toujours passer par une PR depuis `main`.

---

## Recommandations GitHub — Branch Protection

### Branche `main`

- ✅ **Require a pull request before merging** — pas de push direct
- ✅ **Require status checks to pass** — `Lint YAML + Tests unitaires` + `Validate flows on Kestra DEV`
- ✅ **Require branches to be up to date before merging** — évite les conflits

### Branche `prod`

- ✅ **Require a pull request before merging** — pas de push direct
- ✅ **Require status checks to pass** — `Verify PR source is main` (workflow `protect-prod.yml`)
- ✅ **Required approvals** — au moins 1 approbation
- ✅ **Block force pushes** — protège l'historique prod

> Procédure détaillée de configuration : voir `tmp/manual_github_setup_prod_protection.md`

---

## À venir (hors scope actuel)

- Tests et déploiement de la RestAPI FastAPI (`50-docker/api/`)
- Tests et déploiement de la webapp Streamlit (`50-docker/webapp/`)
- Intégration MLflow dans la CI (validation modèles)
