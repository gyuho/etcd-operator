# Store Backup in GCS

We want to have an option to store backup in Google Cloud Storage, or GCS.

## Flags of etcd operator

When staring etcd operator, we need to provide flags in order to access Google Cloud Platform, or GCP:

```bash
$ etcd-operator --backup-gcp-secret ${secret_name} --backup-gcp-config ${configmap_name} --backup-gcs-bucket ${bucket_name} ...
```

Let's explain them one by one:
- "backup-gcp-secret" takes the name of the kube secret object that stores the Google Cloud Platform service account credential JSON file.

First, create and download service account JSON key from https://console.cloud.google.com/apis/credentials.

We can create a secret object by doing:
```bash
$ kubectl create secret generic aws-credential --from-file=${aws_credential_file}
```
"aws-credential" is the ${secret_name}.
The "aws_credential_file" file is documented [here](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#aws-credentials-file-format).
We only use "default" profile.
An example credential file:
```
[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

- "backup-aws-config" takes the name of the kube configmap object that presents the aws config file.

We can create a configmap by doing:
```bash
$ kubectl create configmap aws-config --from-file=${aws_config_file}
```
"aws-config" is the ${configmap_name}.
The "aws_config_file" file is described [here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files).
We only use "default" profile. Note that "region" must be set.
An example config file:
```
[default]
region=us-west-1
```

- "backup-gcs-bucket" takes the name of the s3 bucket in which the operator stores backup.
The backups of each cluster are saved in individual directories under given bucket.
The format would look like: *bucket_name/cluster_name/* .

For example, given bucket "etcd-backups" and if operator has saved backups of cluster "etcd-A", We should see backup files running commands:
```bash
$ aws s3 ls s3://etcd-backups/etcd-A/
```
Or just login into aws console to view it.

## How to create a cluster with backup in GCS

When we create a cluster with backup, we set the backup.storageType to "GCS".
For example, a yaml file would look like:
```
apiVersion: "etcd.coreos.com/v1beta1"
kind: "Cluster"
metadata:
  name: "etcd-cluster-with-backup"
spec:
  ...
  backup:
    ...
    storageType: "GCS"
```
