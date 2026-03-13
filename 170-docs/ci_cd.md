# Pipeline CI/CD — Kestra Flows + Infra Validation

## Vue d'ensemble

| Workflow | Fichier | Déclencheur | Runner |
|----------|---------|-------------|--------|
| **CI** | `.github/workflows/ci.yml` | PR / push vers `main` | `ubuntu-latest` + `[self-hosted, dev]` |
| **Sync + Deploy DEV** | `.github/workflows/sync-dev.yml` | Après CI réussie sur `main` + `workflow_dispatch` | `[self-hosted, dev]` |
| **Protect prod** | `.github/workflows/protect-prod.yml` | PR vers `prod` | `ubuntu-latest` |
| **CD** | `.github/workflows/deploy.yml` | push vers `prod` + `workflow_dispatch` | `[self-hosted, prod]` |

Namespace cible : **`projet713`**

### Branches

| Branche | Rôle | Protégée |
|---------|------|----------|
| `main` | Intégration — code validé par la CI, prêt à être promu | ✅ |
| `prod` | Production — seul le code promu ici est déployé | ✅ |

> **Un merge dans `main` ne déclenche pas de déploiement PROD.** Un merge dans `main` déclenche la CI puis, en cas de succès, le sync + déploiement complet sur Kestra DEV + les smoke tests. La VM DEV est l'environnement de prévalidation : le code y est réellement déployé et testé avant promotion. Le déploiement PROD nécessite une promotion explicite via PR `main → prod`.

### Séparation validation / synchronisation / déploiement

L'architecture CI/CD sépare clairement trois responsabilités :

- **Validation** (`ci.yml`) : vérifications statiques + intégration DEV. Ne modifie aucun fichier sur les VMs.
- **Synchronisation + Déploiement DEV** (`sync-dev.yml`) : copie non destructive des fichiers versionnés vers la VM DEV via `rsync` (sans `--delete`), puis déploiement des namespace files SQL et des flows dans Kestra DEV, suivi de smoke tests (Kestra + apps). Pas de rollback — DEV est l'environnement de prévalidation.
- **Déploiement PROD** (`deploy.yml`) : synchronisation non destructive vers la VM PROD + déploiement Kestra via API + smoke tests + rollback en cas d'échec.

> **Pas de staging / préprod séparé.** La VM DEV est l'environnement complet de prévalidation. La VM PROD est réservée au déploiement réel uniquement. Le workflow de pré-déploiement sur PROD (namespace isolé `projet713-predeploy`) a été supprimé volontairement car l'isolation filesystem n'était pas réelle — les scripts et SQL montés restaient partagés avec la production.

### Séparation infrastructure / pipeline

Le pipeline CI/CD **déploie uniquement les flows Kestra, les namespace files SQL et les scripts Python**. Les services applicatifs (API FastAPI, Webapp Streamlit) et l'infrastructure Kubernetes sont gérés indépendamment par l'équipe infrastructure.

Le pipeline **valide** les artefacts d'infrastructure (Dockerfiles, Helm charts) en CI pour détecter les régressions, mais **ne build, ne push et ne déploie aucune image Docker ni chart Helm**.

---

## CI — `.github/workflows/ci.yml`

S'exécute sur chaque pull request et push vers `main`. Deux jobs : validation statique puis intégration DEV.

### Job 1 : `lint-and-test` — Validation statique (`ubuntu-latest`)

Toutes les vérifications qui ne nécessitent aucun serveur :

#### Code & config

1. **yamllint** — syntaxe YAML, indentation, longueur de ligne sur `10-flows/prod/`
2. **SQL lint** — vérifie que les fichiers dans `140-sql/queries/` sont lisibles
3. **Flow scripts** — vérifie la syntaxe Python (`py_compile`) des scripts métier dans `100-scripts_mlops/`
4. **API source** — vérifie la syntaxe Python (`py_compile`) de tous les fichiers dans `110-api/`
5. **Webapp source** — vérifie la syntaxe Python (`py_compile`) de tous les fichiers dans `120-webapp/`
6. **check_flows.py** — validation custom :
   - champs requis (`id`, `namespace`, `tasks`)
   - namespace = `projet713`
   - pas de flow IDs dupliqués
   - références subflow résolues
   - références `read('...')` pointent vers des fichiers SQL existants
   - clés `kv('...')` dans l'ensemble connu (chargé depuis `kestra_kv_keys.yaml`)
   - pas de secrets hardcodés
7. **pytest** — tests unitaires et d'intégration (`130-tests/`) incluant :
   - `test_flows.py` — validation des flows Kestra
   - `test_api.py` — endpoints FastAPI (`/health`, `/forecast/consumption`), modèles Pydantic
   - `test_webapp.py` — configuration (`API_URL`), client HTTP (mock)
   - `test_config_sync.py` — validité de `repo_structure.yaml`, synchronisation workflows / config
   - `test_smoke_apps.py` — tests unitaires du script de smoke test apps (mocks HTTP)

#### Artefacts d'infrastructure (build-only)

8. **Docker build — API** — `docker build 50-docker/api/build/` (validation Dockerfile + dépendances, aucun push)
9. **Docker build — Webapp** — `docker build 120-webapp/` (validation Dockerfile + dépendances, aucun push)
10. **Helm lint** — `helm lint` sur chaque chart dans `75-infra-prod/*/` (via `azure/setup-helm@v4`)

> **Pourquoi ces Dockerfiles ?** `110-api/` ne contient pas de Dockerfile — le Dockerfile API canonique est dans `50-docker/api/build/`. Pour la webapp, `120-webapp/` est la source de vérité (build context autonome avec Dockerfile + code source). Le répertoire `50-docker/webapp/build/` est un doublon.

### Job 2 : `validate-dev` — Intégration DEV (`[self-hosted, dev]`)

S'exécute **après** `lint-and-test`. Vérifie les composants contre les services actifs sur la VM DEV (705) :

1. **Validate flows Kestra** — `kestra-io/validate-action` contre `http://localhost:8082`
2. **Smoke test apps DEV** — `smoke_test_apps.py --env dev` vérifie :
   - API `http://localhost:8000/health` (Docker Compose)
   - Webapp `http://localhost:8501/_stcore/health` (Docker Compose)

> Ce job ne déploie rien. Il confirme que le code est compatible avec l'environnement DEV actuel.

---

## Sync + Deploy DEV — `.github/workflows/sync-dev.yml`

Synchronise les fichiers versionnés vers la VM DEV, puis déploie les namespace files SQL et les flows dans Kestra DEV, et exécute les smoke tests. Pas de rollback — DEV est un environnement d'expérimentation.

### Déclenchement

- **Automatique** : `workflow_run` déclenché après le succès de la CI (`ci.yml`) sur `main`
- **Manuel** : `workflow_dispatch` pour forcer un sync + déploiement

### Runner

Runner `[self-hosted, dev]` sur la VM DEV (705). Kestra DEV est accessible en `http://localhost:8082`.

### Séquence

1. **Checkout** du dépôt — en mode `workflow_run`, le checkout cible explicitement `github.event.workflow_run.head_sha` (le SHA exact validé par la CI). En mode `workflow_dispatch`, le comportement par défaut (`github.ref`) est utilisé. Cela garantit que la version synchronisée est toujours celle validée par la CI.
2. **Contexte** — affiche trigger, branche, commit, hostname, date, cible, namespace, serveur Kestra
3. **rsync non destructif** (`--archive --compress --verbose`, sans `--delete`). La liste des répertoires est lue depuis `repo_structure.yaml` (`sync.directories`) — source de vérité unique. Répertoires synchronisés vers `~/projet/` :
   - `10-flows/prod/` — flows Kestra
   - `140-sql/` — fichiers SQL
   - `100-scripts_mlops/` — scripts métier
   - `110-api/` — source API FastAPI
   - `120-webapp/` — source Webapp Streamlit
4. **Deploy SQL** — namespace files déployés dans Kestra DEV via `kestra-io/deploy-action` (`resource: namespace_file`)
5. **Validate flows** — validation des flows contre Kestra DEV via `kestra-io/validate-action`
6. **Deploy flows** — déploiement des flows dans Kestra DEV via `kestra-io/deploy-action` (`delete: false`)
7. **Smoke tests Kestra DEV** — exécute `smoke_test_prod.py` avec `KESTRA_SERVER=http://localhost:8082` :
   - flows attendus existent via API
   - namespace files SQL versionnés existent
   - clés KV critiques vérifiées (avertissements, non bloquant — pas de `--strict-kv`)
8. **Smoke tests apps DEV** — exécute `smoke_test_apps.py --env dev` :
   - API `http://localhost:8000/health`
   - Webapp `http://localhost:8501/_stcore/health`

### Ordre important

Le SQL (namespace files) doit être déployé **avant** les flows, car certains flows font `read('queries/linky_gold.sql')`.

### Garanties

- **Non destructif** : `rsync` sans `--delete` + `delete: false` pour les flows — les fichiers et flows ajoutés manuellement sur la VM DEV ne sont jamais supprimés
- **Pas de rollback** : DEV est un environnement d'expérimentation. En cas d'échec, le workflow échoue visiblement mais l'état est conservé pour investigation
- **Condition** : en mode automatique, le sync ne s'exécute que si la CI a réussi (`workflow_run.conclusion == 'success'`)
- **Version exacte** : le checkout utilise `head_sha` pour garantir la correspondance entre la version validée par la CI et la version déployée

---

## CD — `.github/workflows/deploy.yml`

Déploie sur Kestra prod quand du code est **promu sur la branche `prod`**.

### Déclenchement

- `push` vers `prod` avec filtre `paths` sur `10-flows/prod/**`, `140-sql/queries/**`, `100-scripts_mlops/**`, `110-api/**`, `120-webapp/**`
- `workflow_dispatch` pour déclenchement manuel

### Runner

Runner `[self-hosted, prod]` sur la VM de production (713). Kestra est accessible en `http://localhost:30082`.

### Release unitaire

L'ensemble du déploiement PROD est traité comme **une seule unité de release** :

```
sync → deploy Kestra → smoke tests (Kestra + apps) → rollback si échec
```

Tout se déroule dans un seul job. Si **n'importe quelle** étape échoue (sync, deploy SQL, deploy flows, smoke tests Kestra, smoke tests apps), le rollback est déclenché automatiquement.

### Séquence

1. **Checkout** avec `fetch-depth: 2` (pour permettre le rollback)
2. **Contexte** — affiche branche, commit, hostname, date, namespace, serveur
3. **Sync filesystem** — `rsync` non destructif (sans `--delete`). La liste des répertoires est lue depuis `repo_structure.yaml` (`sync.directories`) — source de vérité unique. Répertoires synchronisés vers `~/projet/` :
   - `10-flows/prod/` — flows Kestra
   - `140-sql/` — fichiers SQL
   - `100-scripts_mlops/` — scripts métier
   - `110-api/` — source API FastAPI
   - `120-webapp/` — source Webapp Streamlit
4. **Deploy SQL** — namespace files via `kestra-io/deploy-action`
5. **Validate flows** — via `kestra-io/validate-action`
6. **Deploy flows** — via `kestra-io/deploy-action` (`delete: false`)
7. **Smoke tests Kestra** — exécute `smoke_test_prod.py` qui lit la configuration depuis `deploy_smoke_tests.yaml` et vérifie :
   - les flows attendus existent via API
   - tous les namespace files SQL versionnés dans Git existent
   - les clés KV critiques sont accessibles
8. **Smoke tests apps** — exécute `smoke_test_apps.py --env prod` qui vérifie :
   - API `/health` sur port 30088
   - Webapp `/_stcore/health` sur port 30085
9. **Notification Discord** (succès — Kestra + apps)
10. **Rollback** en cas d'échec — re-sync + redéploie les fichiers du commit précédent
11. **Notification Discord** (échec + rollback)

### Ordre important

Le SQL (namespace files) doit être déployé **avant** les flows, car `mqtt_linky_gold` fait `read('queries/linky_gold.sql')`.

### Centralisation des répertoires synchronisés

La liste des répertoires synchronisés (`sync.directories` dans `repo_structure.yaml`) est la **source de vérité unique** pour les étapes de sync filesystem dans `sync-dev.yml`, `deploy.yml` et `rollback_prod.sh`. Les workflows lisent cette liste dynamiquement via un one-liner Python, évitant toute duplication entre le fichier de config et les scripts shell.

### Garanties de déploiement non destructif

Le pipeline CD est conçu pour être **conservatif** :

- **Sync filesystem** : `rsync` sans `--delete` — les fichiers présents sur la VM PROD mais absents du repo Git ne sont **pas supprimés**. Seuls les fichiers versionnés sont créés ou mis à jour.
- **Flows** : `delete: false` garantit que les flows déjà présents sur Kestra mais absents du repo Git ne sont **pas supprimés**. Seuls les flows versionnés dans Git sont créés ou mis à jour.
- **Namespace files (SQL)** : `kestra-io/deploy-action` avec `resource: namespace_file` crée ou met à jour les fichiers présents dans Git. Les namespace files déjà sur Kestra mais absents du dépôt ne sont **pas affectés**.
- **Rollback** : en cas d'échec, le script `rollback_prod.sh` utilise `git ls-tree` + `git show` pour énumérer et restaurer **tous** les fichiers qui existaient à `HEAD~1`, y compris les fichiers **supprimés** entre les deux commits. Les ressources Kestra (flows + SQL) sont re-déployées via API, les scripts sont restaurés sur disque, puis le tout est re-synchronisé vers `~/projet/`. Le rollback ne supprime rien.

> **Principe** : Git est la source de vérité pour les ressources qu'il gère, mais le déploiement ne touche jamais aux ressources non gérées par Git.

---

## Secrets GitHub requis

| Secret | Usage | Obligatoire |
|--------|-------|-------------|
| `KESTRA_ADMIN_USER` | Authentification API Kestra (CI + CD) | ✅ |
| `KESTRA_ADMIN_PASS` | Authentification API Kestra (CI + CD) | ✅ |
| `DISCORD_WEBHOOK_URL` | Notifications Discord (CD) | Recommandé |

> **Note** : les secrets `DOCKERHUB_USERNAME` et `DOCKERHUB_TOKEN` ne sont pas nécessaires. Le pipeline ne push aucune image Docker.

---

## Artefacts versionnés

### Déployés par le pipeline CD

| Type | Répertoire | Déployé via | Sync VM | Rollback |
|------|-----------|-------------|---------|----------|
| **Flows Kestra** | `10-flows/prod/` | `kestra-io/deploy-action` (API) | rsync → `~/projet/` | `git ls-tree` + API PUT par flow |
| **Namespace files SQL** | `140-sql/queries/` | `kestra-io/deploy-action` (API) | rsync → `~/projet/` | `git ls-tree` + API POST par fichier |
| **Flow scripts Python** | `100-scripts_mlops/` | rsync vers VM | rsync → `~/projet/` | `git ls-tree` + `git show HEAD~1` |
| **API source** | `110-api/` | rsync vers VM | rsync → `~/projet/` | rsync rollback |
| **Webapp source** | `120-webapp/` | rsync vers VM | rsync → `~/projet/` | rsync rollback |

### Validés en CI uniquement (non déployés)

| Type | Emplacement canonique | Validation CI |
|------|----------------------|---------------|
| **Dockerfile API** | `50-docker/api/build/` | `docker build` (build-only) |
| **Dockerfile Webapp** | `120-webapp/` | `docker build` (build-only) |
| **Helm chart API** | `75-infra-prod/energy-api/` | `helm lint` |
| **Helm chart Webapp** | `75-infra-prod/energy-webapi/` | `helm lint` |
| **Helm chart Kestra** | `75-infra-prod/kestra/` | `helm lint` |

Les outils CI/CD (`ci/`, `deploy/`, `config/`) sont regroupés dans `95-ci-cd/`, séparés des scripts métier qui résident directement dans `100-scripts_mlops/`. Cette séparation physique élimine tout besoin d'exclusion dans les étapes de validation ou de rollback.

---

## Fichiers de configuration

### `95-ci-cd/config/repo_structure.yaml`

**Source de vérité centralisée des chemins et constantes du pipeline.** Sections :

| Section | Contenu |
|---------|---------|------|
| `directories` | Chemins des répertoires (flows, sql, scripts, cicd, api, webapp, tests) |
| `kestra` | Namespace, serveurs dev et prod |
| `docker` | Noms d'images Docker, chemins des build contexts canoniques (API: `50-docker/api/build`, Webapp: `120-webapp`) |
| `infra` | Chemin du répertoire des Helm charts (`75-infra-prod`) |
| `sync` | Chemin cible (`~/projet/`) et liste des répertoires à synchroniser vers les VMs |
| `config_files` | Chemins relatifs des fichiers de configuration (kv_keys, smoke_tests, apps_smoke_tests) |

Les scripts Python (`check_flows.py`, `smoke_test_prod.py`, `smoke_test_apps.py`) chargent ce fichier via le module `load_config.py`. Les blocs `env:` des workflows GitHub Actions (`ci.yml`, `sync-dev.yml`, `deploy.yml`) **doivent** refléter les mêmes valeurs — un test pytest (`test_config_sync.py`) vérifie automatiquement la synchronisation.

### `95-ci-cd/config/deploy_apps_smoke_tests.yaml`

**Configuration des health checks applicatifs pour DEV et PROD.** Structure à deux sections (`dev`, `prod`), chacune avec une liste `endpoints` contenant pour chaque service : `name`, `url`, `health_path`, `required`.

Utilisé par `smoke_test_apps.py --env <dev|prod>`.

### `95-ci-cd/config/load_config.py`

**Module partagé de chargement de la configuration.** Expose `load_repo_structure()` qui retourne le contenu de `repo_structure.yaml` sous forme de dict Python.

### `kestra_kv_keys.yaml` (racine)

**Contrat complet des clés KV du système.** Contient toutes les clés attendues dans le namespace `projet713`. Utilisé par `check_flows.py` en CI pour vérifier que les `kv('...')` référencés dans les flows correspondent à des clés connues.

Pour ajouter une nouvelle clé KV : l'ajouter dans `kestra_kv_keys.yaml` et commiter.

### `95-ci-cd/config/deploy_smoke_tests.yaml`

**Configuration des smoke tests de déploiement Kestra.** Sous-ensemble des vérifications exécutées post-deployment par `smoke_test_prod.py` :

- `expected_flows` — liste des flows qui doivent exister après déploiement
- `critical_kv_keys` — clés KV critiques à vérifier (sous-ensemble de `kestra_kv_keys.yaml`)

Ce fichier est la **source de vérité des smoke tests CD Kestra**. Pour modifier les vérifications post-déploiement, éditer ce fichier — pas le workflow.

### `95-ci-cd/config/deploy_apps_smoke_tests.yaml`

**Configuration des smoke tests applicatifs (API + Webapp).** Définit les endpoints à vérifier post-deployment par `smoke_test_apps_prod.py` :

- `endpoints` — liste d'endpoints avec `name`, `url`, `health_path`, `required`

Pour ajouter un service à vérifier : ajouter une entrée dans la liste `endpoints` — pas dans le workflow.

### Séparation des responsabilités

| Fichier | Rôle | Utilisé par |
|---------|------|-------------|
| `repo_structure.yaml` | Chemins, constantes, conventions (CI + CD) | `check_flows.py`, `smoke_test_prod.py`, `smoke_test_apps.py`, `test_config_sync.py` |
| `kestra_kv_keys.yaml` | Contrat complet des KV (CI) | `check_flows.py` |
| `deploy_smoke_tests.yaml` | Vérifications post-deploy Kestra (CD) | `smoke_test_prod.py` |
| `deploy_apps_smoke_tests.yaml` | Vérifications santé apps (CD) | `smoke_test_apps.py` |

---

## Scripts de déploiement

### `smoke_test_prod.py`

Script Python exécuté après le déploiement Kestra (DEV et PROD). Lit `deploy_smoke_tests.yaml`, interroge l'API Kestra via HTTP (stdlib `urllib`), et vérifie :

1. Chaque flow attendu existe (`GET /api/v1/main/flows/{ns}/{id}`)
2. Les namespace files SQL versionnés existent (`GET /api/v1/main/namespaces/{ns}/files`)
3. Les clés KV critiques sont accessibles (`GET /api/v1/main/namespaces/{ns}/kv/{key}`)

Aucune dépendance externe n'est nécessaire (Python stdlib + PyYAML).

Variables d'environnement requises : `KESTRA_SERVER`, `KESTRA_NAMESPACE`, `KESTRA_USER`, `KESTRA_PASS`, `SQL_DIR`.

### `smoke_test_apps.py`

Script Python exécuté en CD pour vérifier la santé des services applicatifs. Lit `deploy_apps_smoke_tests.yaml` et effectue des health checks HTTP sur chaque endpoint configuré. Ne nécessite aucune authentification (endpoints publics).

Aucune dépendance externe (Python stdlib + PyYAML). Aucune variable d'environnement requise.

### `rollback_prod.sh`

Script de rollback utilisant `git ls-tree` + `git show` pour restaurer l'état de `HEAD~1`.

### `deploy_flows.sh` (legacy)

Conservé comme outil de **débogage manuel / fallback d'urgence** uniquement. Non utilisé dans les workflows CI/CD.

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
feature → PR vers main → CI → merge → sync + deploy DEV → vérification → PR main→prod → approbation → merge → sync + deploy PROD
```

1. Créer une branche feature : `git checkout -b feature/mon-changement`
2. Modifier les flows, SQL ou scripts
3. Commiter, pousser, ouvrir une PR vers `main`
4. La CI s'exécute automatiquement (yamllint, check_flows, pytest, validate-action sur DEV)
5. Si la CI passe → merge dans `main`
6. Le sync + deploy DEV se déclenche automatiquement :
   - rsync non destructif vers la VM DEV
   - déploiement SQL + flows dans Kestra DEV
   - smoke tests Kestra DEV + apps DEV
7. Vérifier que le déploiement DEV est correct (le workflow DEV est l'environnement de prévalidation)
8. Quand le code est prêt pour la prod, ouvrir une **PR de `main` vers `prod`** via GitHub
9. Le workflow `protect-prod.yml` vérifie que la source est bien `main`
10. Un pair approuve la PR
11. Merge la PR → le push sur `prod` déclenche le sync PROD + CD
12. Smoke tests + notification Discord

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

- Intégration MLflow dans la CI (validation modèles)
- Tests d'intégration webapp Streamlit (UI / Selenium)
