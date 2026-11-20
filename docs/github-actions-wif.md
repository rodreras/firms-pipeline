# Google Cloud Workload Identity Federation for GitHub Actions

The FIRMS daily and backfill workflows use GitHub’s OIDC token to impersonate
a Google Cloud service account. This setup uses an existing Google Cloud project
and existing GCS destinations. Run the commands in Bash with a signed-in
`gcloud` account that can enable APIs, create IAM resources, and change IAM
policies on the service account and storage destinations.
If needed, sign in first with `gcloud auth login`.

## 1. Create the service account, pool, and provider

Replace the project ID and the two numeric GitHub IDs. For this public
repository, find `id` and `owner.id` in the
[GitHub repository API response](https://api.github.com/repos/rodreras/firms-pipeline).
Numeric IDs keep the trust policy tied to the repository and owner even if their
names change. The example trusts publishing from `main` in `firms-production`.

```bash
PROJECT_ID="your-gcp-project"
GITHUB_REPOSITORY_ID="your-numeric-repository-id"
GITHUB_OWNER_ID="your-numeric-owner-id"
POOL_ID="github-actions"
PROVIDER_ID="firms-publisher"
ACCOUNT_ID="firms-publisher"
SERVICE_ACCOUNT="${ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud services enable iam.googleapis.com iamcredentials.googleapis.com \
  sts.googleapis.com cloudresourcemanager.googleapis.com storage.googleapis.com \
  --project="$PROJECT_ID"

gcloud iam service-accounts create "$ACCOUNT_ID" \
  --display-name="FIRMS GitHub Actions publisher" --project="$PROJECT_ID"

gcloud iam workload-identity-pools create "$POOL_ID" \
  --location=global --display-name="GitHub Actions" --project="$PROJECT_ID"

gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
  --location=global --workload-identity-pool="$POOL_ID" --project="$PROJECT_ID" \
  --display-name="FIRMS GitHub publisher" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository_id=assertion.repository_id" \
  --attribute-condition="assertion.repository_owner_id == '${GITHUB_OWNER_ID}' && assertion.repository_id == '${GITHUB_REPOSITORY_ID}' && assertion.ref == 'refs/heads/main' && assertion.environment == 'firms-production'"
```

Use a pool dedicated to this trust boundary. The service-account binding below
matches identities across the pool, so other providers in the same pool must
also be trusted to assert the mapped repository ID.
[Google’s GitHub Actions authentication guide](https://github.com/google-github-actions/auth#workload-identity-federation-through-a-service-account)

## 2. Grant impersonation and storage access

`roles/iam.workloadIdentityUser` lets the matching GitHub identity impersonate
the service account. `roles/storage.objectUser` lets the service account read,
create, and replace objects. GCS requires delete permission to replace existing
objects, including the annual files.

```bash
POOL_RESOURCE="$(gcloud iam workload-identity-pools describe "$POOL_ID" \
  --location=global --project="$PROJECT_ID" --format='value(name)')"

gcloud iam service-accounts add-iam-policy-binding "$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/${POOL_RESOURCE}/attribute.repository_id/${GITHUB_REPOSITORY_ID}"

PUBLIC_URI="gs://your-public-bucket/firms"
PRIVATE_URI="gs://your-private-bucket/firms-operations"

# For separate buckets dedicated to this pipeline, use the bucket roots here.
for bucket in gs://your-public-bucket gs://your-private-bucket; do
  gcloud storage buckets add-iam-policy-binding "$bucket" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" --role=roles/storage.objectUser
done
```

For shared buckets, skip the bucket grants above and grant the role on each
existing managed folder. Set the two URI variables to the actual prefixes.
Managed folders require uniform bucket-level access.

```bash
for folder in "$PUBLIC_URI" "$PRIVATE_URI"; do
  gcloud storage managed-folders add-iam-policy-binding "${folder%/}/" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" --role=roles/storage.objectUser
done
```

Use distinct, non-overlapping prefixes. Configure public reader access
separately and grant it only where the dataset is published.
[Cloud Storage IAM roles](https://cloud.google.com/storage/docs/access-control/iam-roles) · [Managed folder IAM command](https://cloud.google.com/sdk/gcloud/reference/storage/managed-folders/add-iam-policy-binding)

## 3. Configure the GitHub environment

Print the two Google identifiers for GitHub. The provider resource name
contains the **project number**, which the describe command supplies.

```bash
gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
  --location=global --workload-identity-pool="$POOL_ID" --project="$PROJECT_ID" \
  --format='value(name)'
printf '%s\n' "$SERVICE_ACCOUNT"
```

In GitHub, open **Settings → Environments → firms-production**, creating
the environment if needed. Restrict deployment branches to `main` to match
the provider condition. Add these entries in the indicated sections.

| Section | Name | Value |
| --- | --- | --- |
| Environment variables | `GCP_WORKLOAD_IDENTITY_PROVIDER` | Full provider resource name from the describe command |
| Environment variables | `GCP_SERVICE_ACCOUNT` | Service account email |
| Environment secrets | `FIRMS_MAP_KEY` | NASA FIRMS MAP key |
| Environment secrets | `GCLOUD_URI` | Public dataset URI, the value of `PUBLIC_URI` |
| Environment secrets | `GCLOUD_PRIVATE_URI` | Private operations URI, the value of `PRIVATE_URI` |

The publishing job uses `environment: firms-production` and
`id-token: write`. Keep `secrets: inherit` on both the daily and backfill
caller jobs. Adding it there resolved the empty FIRMS and GCS values in this
repository. [Publishing workflow](../.github/workflows/publish.yml) · [Daily caller](../.github/workflows/daily.yml) · [Backfill caller](../.github/workflows/backfill.yml)

## 4. Verify with a small run

Allow a few minutes for IAM changes to propagate. When ready to publish, dispatch
**FIRMS daily ingestion** from `main` with both dates set to the same completed
UTC day. The run can write to the configured GCS destinations.

- If **google-github-actions/auth** fails, check the provider name, service
  account email, provider condition, and impersonation binding.
- If **Ingest and publish** reports an empty variable, check that all three
  entries are under **Environment secrets** and that the caller uses
  `secrets: inherit`. Google authentication can succeed using environment
  variables even when the publish secrets arrive empty.
- If GCS returns **403**, check the service account's object role on the
  destination that failed, including the private prefix.
