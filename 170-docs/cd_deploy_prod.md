# Déploiement continu — Flows Kestra (PROD)

## Déclencheurs

- **Push sur `prod`** avec modifications dans `10-flows/prod/**`, `140-sql/queries/**` ou `100-scripts_mlops/**` (hors `ci/`, `deploy/`)
- **Dispatch manuel** via l'interface GitHub Actions (`workflow_dispatch`)

Le déploiement prod ne se déclenche **pas** depuis `main`. Le code doit être explicitement promu vers la branche `prod`.

## Runner

Runner `[self-hosted, prod]` sur la VM de production (713). Kestra est accessible en `http://localhost:30082`.

## Séquence de déploiement

1. Checkout du dépôt (avec `fetch-depth: 2` pour le rollback)
2. Affichage du contexte (branche, commit, hostname, date)
3. Déploiement des namespace files SQL via `kestra-io/deploy-action`
4. Validation des flows via `kestra-io/validate-action`
5. Déploiement des flows via `kestra-io/deploy-action` (`delete: false`)
6. Smoke tests (flows existent, namespace file présent, clés KV accessibles)
7. Notification Discord (succès ou échec + rollback)

## Rollback

En cas d'échec, le script `rollback_prod.sh` restaure automatiquement **toutes les ressources versionnées dans Git** (flows, fichiers SQL, scripts Python) à leur version précédente (`HEAD~1`). Il utilise `git ls-tree` + `git show` pour énumérer explicitement chaque fichier de `HEAD~1` et le restaurer, y compris les fichiers **supprimés** entre les deux commits (ce que `git checkout HEAD~1 -- <dir>` ne gère pas). Les flows et SQL sont re-déployés via l'API Kestra ; les scripts sont restaurés sur disque. Le rollback ne supprime aucune ressource.

## Garanties non destructives

- **Flows** : `delete: false` — les flows déjà sur Kestra mais absents du repo ne sont pas supprimés
- **Namespace files** : seuls les fichiers versionnés dans Git sont créés/mis à jour ; les fichiers hors Git sont laissés intacts
- **Flow scripts** : les scripts Python dans `100-scripts_mlops/` (hors `ci/`, `deploy/`) sont présents sur la VM via le checkout Git — pas de déploiement API
- **Rollback** : ne restaure que les ressources trackées par Git ; ne touche à rien d'autre

## Secrets

| Secret               | Description                        | Obligatoire |
|----------------------|------------------------------------|-------------|
| `KESTRA_ADMIN_USER`  | Nom d'utilisateur admin Kestra     | ✅ |
| `KESTRA_ADMIN_PASS`  | Mot de passe admin Kestra          | ✅ |
| `DISCORD_WEBHOOK_URL`| Webhook Discord pour notifications | Recommandé |

## Script legacy

L'ancien `100-scripts_mlops/deploy/deploy_flows.sh` est conservé pour débogage/urgence uniquement. Le CI/CD utilise les GitHub Actions officielles Kestra.
