# kubemr
Map reduce PoC. working title

## PoC

Create a S3 bucket, store the credentials as a secret

    kubectl create secret generic kubemr-operator --from-literal=s3-secret=<S3_SECRET_ACCESS_KEY> --from-literal=s3-access=<S3_ACCESS_KEY_ID>

Create a configuration...

Edit [operator-cm.yaml](manifests/operator-cm.yaml) then apply

    kubectl create -f manifests/operator-cm.yaml

Launch the operator

    kubectl create -f manifests/operator-deployment.yaml

At this point the operator might crash a few times, because it takes some time for TPR to initialize. I could write code to wait for it.

Create a job

    kubectl create -f manifests/wordcount.yaml

The output should look something like `mapreducejob "test-6vkjr" created` where `6vkjr` portion is dynamically generated.
IMPORTANT: Do not edit the job once created or bad things might happen.

Watch all jobs

    kubectl get mapreducejobs --all-namespaces -o=custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status -w

Check on the particular jobs progress

    kubectl get mapreducejobs test-6vkjr -o json

If the status is `COMPLETE`, then look at the `results` list, fetch them somehow, if needed merge them into single file.

## Background

A few years ago I did a similar [PoC](https://github.com/turbobytes/gomr), using etcd for locking/consensus, and uploading/downloading worker binaries from S3.

kubemr is similar, but we (ab)use Kubernetes [Third Party Resources](https://kubernetes.io/docs/concepts/ecosystem/thirdpartyresource/) for state, docker images for workers, and optionally S3 for storing stage outputs. Currently it only supports Amazon S3, but should be trivial to support other S3-like services.

## State

All state for a job is stored in a TPR. Originally we planned to use etcd for state, but we decided to use [JSON patch](http://jsonpatch.com/) functionality [provided by kubernetes](https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#patch-operations) to make changes to this state. The `test` operation allows the patch to fail if some condition is not met.

Example lock using JSON patch :-

```
[
  { "op": "test", "path": "/lockholder", "value": None },
  { "op": "add", "path": "/lockholder", "value": "me" },
]
```

Above operation would fail if the `lockholder` already had a value. So multiple users might try to aquire the lock but only 1 would succeed.

## Worker images

Worker images are normal docker images that must have `CMD` or `ENTRYPOINT` defined. See [wordcount example](cmd/wordcount/) to see how the binary should be implemented.

At the base level, all map/reduce inputs/outputs and results are strings. Helper functions are provided to upload/download these from S3. Your worker code can interpret this as anything - database table/keys, some other storage provider, shared filesystem, etc...

## Operator

The operator registers a TPR called `MapReduceJob` and deploys a kubernetes [Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/) with the worker image specified. Once a Job is deployed, the workers manage the state. It should be safe to run multiple replicas of the operator.

All resources for a MapReduceJob is created in the namespace the MapReduceJob is created in.

## API server

TODO: Not yet implemented

## Notes:-

1. This is not robust code. Do not use in production.
2. Do not edit the `MapReduceJob` after creation unless you really know what you are doing.
3. There is no failure retry.
4. Currently I am not cleaning up after a job is finished. For testing deploy the `MapReduceJob` in a new namespace and delete that entire namespace when done.
5. [2017-kubecon-eu](https://github.com/arschles/2017-KubeCon-EU) - Very helpful. I came across the talk after I started kubemr.
6. Highly likely to have backwards-incompatible changes.
