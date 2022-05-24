# Placement controller

This is a controller which assists with node placement hints. It watches and
consumes node labels of the form `requested-placement-policy`, attempts to apply
the policy, and generates node labels of the form `applied-placement-policy`.
These labels can be used along with pod spread topologies in order to improve
service resiliance. This labels can be easily applied as part of node pool
creation.

## Why does this exist?

This controller exists because using placement policies directly imposes many
difficult-to-handle edge cases and the desired semantics are unclear. Surge
upgrades, auto-provisioning, auto-scaling might stop working as expected.
However, some users may still wish to use placement policies with GKE.

Because placement policies can be applied after instances exist, this controller
allows these policies to be applied without disrupting core functionality. The
edge cases (typically hit when the placement policy instance size limit has been
hit) will be handled with standard Kubernetes scheduling semantics.

## Usage

Users must express an appropriate `requested-placement-policy` label when
creating a node pool. After this, the controller will attempt to apply the
placement policy to nodes (not always successfully), and the user must determine
the semantics of how this constraints their pod scheduling.

To give an example, a user may choose to use
`requiredDuringSchedulingIgnoredDuringExecution` with respect to
`applied-placement-policy` to express a hard preference for nodes that have the
placement policy applied. They may also choose to apply that to
`requested-placement-policy` that will only schedule on nodes that *should* have
the placement policy applied eventually. Finally, they may express any
combination, such as `preferredDuringSchedulingIgnoredDuringExecution` on the
`applied-placement-policy` and the hard constraint on the label
`requested-placement-polciy`. In almost all cases, the user should also have a
spread topology configured with respect to `kubernetes.io/hostname`, in order to
ensure that pods are actually spread across the placement policy.

## Limitations

The use of placement policies is subject to several constraints:

*   Placement policies must already exist.
*   Nodes with placement policy must not be preemptible.
*   Nodes with placement policy must be MIGRATE on maintenance events.
*   Only spread placement policies are supported.

## Specification of a placement policy

This controller accepts the specification of placement policies in three
different forms.

*   `placement-policy`: This is automatically expanded to
    `project/instance-zone/placement-policy`.
*   `project/placement-policy`: This is automatically expanded to
    `project/instance-zone/placement-policy`.
*   `project/zone/placement-policy`: This is used as is. Note that this will be
    ignored if the instance zone is different from the specified zone.

## Failure conditions

There are many reasons that a placement policy may fail to apply, for example:

*   The limit of the placement policy could be reached.
*   The instance may not support placement policies.
*   The zone could be wrong.
*   The API might be broken.

In these cases, the `applied-placement-policy` is simply not updated. The
behavior of the cluster depends on the user configuration.

However, some of these failures might be transient. In these failure cases, the
application of the placement policy is retried in an exponentation backoff.
Since subsequent requests are unlikely to succeed (but may), the backoff will
proceed until a full week between attempts. For a large cluster, where up to
15,000 nodes might be misconfigured, this would result in an attempt to set the
placement policy on a node every 40 seconds.

## Installation

**These instructions assume [Workload
Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
for GKE**.

### Permissions

Before using this controller an appropriate service account must be created that
is able to apply the placement policy to instances.

To start, select the appropriate project and a service account name:

```bash
export GSA_NAME=placement-controller
export GSA_PROJECT=gsa-project
export COMPUTE_PROJECT=compute-project
export K8S_NAMESPACE=placement-controller
```

**Using a different K8S_NAMESPACE may require modifications to
`deployment.yaml`**.

Then ensure these permissions are available on the relevant project, noting that
you may need to do this in different `${COMPUTE_PROJECT}`s as required:

```bash
gcloud iam service-accounts create "${GSA_NAME}" --project="${GSA_PROJECT}"
gcloud projects add-iam-policy-binding "${COMPUTE_PROJECT}" --member "serviceAccount:${GSA_NAME}@${GSA_PROJECT}.iam.gserviceaccount.com" --role "roles/compute.instanceAdmin.v1"
```

Now, allow the service to use the service account using workload identity:

```bash
kubectl create namespace "${K8S_NAMESPACE}"
kubectl create serviceaccount "placement-controller" --namespace "${K8S_NAMESPACE}"
gcloud iam service-accounts add-iam-policy-binding "${GSA_NAME}@${GSA_PROJECT}.iam.gserviceaccount.com" --role roles/iam.workloadIdentityUser --member "serviceAccount:${COMPUTE_PROJECT}.svc.id.goog[${K8S_NAMESPACE}/placement-controller]"
kubectl annotate serviceaccount "placement-controller" --namespace "${K8S_NAMESPACE}" iam.gke.io/gcp-service-account="${GSA_NAME}@${GSA_PROJECT}.iam.gserviceaccount.com"
```

### Building and deploying

This repository contains a sample `deployment.yaml` that can be built with
[ko](https://github.com/google/ko). To start, `ko` must be installed:

```go
go install github.com/google/ko@latest
```

After the permissions have been setup appropriately, the deployment can be
created or updated with providing an appropriate `KO_DOCKER_REPO` and running:


```bash
kubectl config set-context --current --namespace="${K8S_NAMESPACE}"
KO_DOCKER_REPO=gcr.io/my-project/placement-controller ko resolve -f deployment.yaml | kubectl apply -f -

```
