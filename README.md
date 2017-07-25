# kubemr
Map reduce in Kubernetes

Completely rewritten the logic. View [old readme](https://github.com/turbobytes/kubemr/blob/df4be28f732a0fb75611e599561d689b301b151f/README.md) for previous version. Turns out for large jobs frequent patches to TPR is not sustainable.

## PoC

Create a S3 bucket, store the credentials as a secret

    kubectl create secret generic aws --from-literal=aws_secret_access_key=<S3_SECRET_ACCESS_KEY> --from-literal=aws_access_key_id=<S3_ACCESS_KEY_ID>

Edit `KUBEMR_` variables [wordcount.yaml](manifests/wordcount.yaml) then create

    kubectl create -f manifests/wordcount.yaml

This launches a pod, which acts as the master for the job. It creates workers, based on a pod template. View logs of this pod to keep track of the progress.

## Background

A few years ago I did a similar [PoC](https://github.com/turbobytes/gomr), using etcd for locking/consensus, and uploading/downloading worker binaries from S3.


## State

All state for a job is stored in the object created by usercode. The `MapReduceJob` creates a http server locally and manages locking.

## Worker images

Worker images are normal docker images that must have `CMD` or `ENTRYPOINT` defined. See [wordcount example](cmd/wordcount/) to see how the binary should be implemented.

At the base level, all map/reduce inputs/outputs and results are strings. Helper functions are provided to upload/download these from S3. Your worker code can interpret this as anything - database table/keys, some other storage provider, shared filesystem, etc...

## Notes:-

1. This is not robust code. Do not use in production.
2. Do not edit the `MapReduceJob` after creation unless you really know what you are doing.
3. There is no failure retry.
4. Currently I am not cleaning up after a job is finished. For testing deploy the `MapReduceJob` in a new namespace and delete that entire namespace when done.
5. [2017-kubecon-eu](https://github.com/arschles/2017-KubeCon-EU) - Very helpful. I came across the talk after I started kubemr.
6. Highly likely to have backwards-incompatible changes.
7. I am not completely sure about atomic guarantees of using JSON patch on kubernetes apiserver.
8. As of now, Kubernetes does not provide a way to enforce a particular schema to ThirdPartyResources. So if an invalid schema is submitted, operator will fail to validate the `MapReduceJob` and mark the status as `FAIL`.
