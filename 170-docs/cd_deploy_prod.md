# Déploiement continu — Flows Kestra (PROD)

## Déclencheurs

- **Push sur `prod`** avec modifications dans `10-flows/prod/**`, `140-sql/queries/**`, `100-scripts_mlops/**`, `110-api/**` ou `120-webapp/**`
- **Dispatch manuel** via l'interface GitHub Actions (`workflow_dispatch`)

Le déploiement prod ne se déclenche **pas** depuis `main`. Le code doit être explicitement promu vers la branche `prod`.

> **Prévalidation** : avant la promotion, le code est automatiquement déployé et testé sur la VM DEV (sync + deploy Kestra DEV + smoke tests) via `sync-dev.yml`. La VM DEV est l'environnement complet de prévalidation. Voir [ci_cd.md](ci_cd.md) pour plus de détails.

## Runner

Runner `[self-hosted, prod]` sur la VM de production (713). Kestra est accessible en `http://localhost:30082`.

## Séquence de déploiement

1. Checkout du dépôt (avec `fetch-depth: 2` pour le rollback)
2. Affichage du contexte (branche, commit, hostname, date)
3. **Sync filesystem** — rsync non destructif (`--archive --compress --verbose`, sans `--delete`) des répertoires versionnés vers `~/projet/` sur la VM PROD :
   - `10-flows/prod`, `140-sql`, `100-scripts_mlops`, `110-api`, `120-webapp`
4. Déploiement des namespace files SQL via `kestractl nsfiles upload`
5. Validation des flows via `kestractl flows validate`
6. Déploiement des flows via `kestractl flows deploy` (`--override`)
7. Smoke tests (flows existent, namespace file présent, clés KV accessibles)
8. Notification Discord (succès ou échec + rollback)

## Rollback

En cas d'échec, le script `rollback_prod.sh` restaure automatiquement **toutes les ressources versionnées dans Git** (flows, fichiers SQL, scripts Python) à leur version précédente (`HEAD~1`). Il utilise `git ls-tree` + `git show` pour énumérer explicitement chaque fichier de `HEAD~1` et le restaurer, y compris les fichiers **supprimés** entre les deux commits (ce que `git checkout HEAD~1 -- <dir>` ne gère pas). Les flows et SQL sont re-déployés via l'API Kestra ; les scripts sont restaurés sur disque.

Après la restauration des fichiers, le script **re-synchronise** les répertoires restaurés vers `$SYNC_TARGET` (par défaut `~/projet/`) via rsync, garantissant que la VM de production reflète l'état `HEAD~1`. Le rollback ne supprime aucune ressource.

## Garanties non destructives

- **Flows** : `delete: false` — les flows déjà sur Kestra mais absents du repo ne sont pas supprimés
- **Namespace files** : seuls les fichiers versionnés dans Git sont créés/mis à jour ; les fichiers hors Git sont laissés intacts
- **Sync filesystem** : `rsync` sans `--delete` — les fichiers déjà sur la VM mais absents du repo ne sont pas supprimés
- **Flow scripts** : les scripts Python dans `100-scripts_mlops/` sont synchronisés sur la VM via rsync vers `~/projet/`
- **Rollback** : ne restaure que les ressources trackées par Git, puis re-synchronise vers la VM ; ne touche à rien d'autre

## Secrets

| Secret / Variable    | Description                              | Obligatoire |
|----------------------|------------------------------------------|-------------|
| `KESTRA_ADMIN_USER`  | Nom d'utilisateur admin Kestra           | ✅ |
| `KESTRA_ADMIN_PASS`  | Mot de passe admin Kestra                | ✅ |
| `DISCORD_WEBHOOK_URL`| Webhook Discord pour notifications       | Recommandé |
| `SYNC_TARGET`        | Chemin cible sur la VM (`~/projet/`)     | ✅ (env) |

## Script legacy

L'ancien `95-ci-cd/deploy/deploy_flows.sh` est conservé pour débogage/urgence uniquement. Le CI/CD utilise les GitHub Actions officielles Kestra.
