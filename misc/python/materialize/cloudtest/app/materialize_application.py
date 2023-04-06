# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import time
from datetime import datetime, timedelta
from typing import List, Optional

from pg8000.exceptions import InterfaceError

from materialize.cloudtest.app.cloudtest_application_base import (
    CloudtestApplicationBase,
)
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.cockroach import cockroach_resources
from materialize.cloudtest.k8s.debezium import debezium_resources
from materialize.cloudtest.k8s.environmentd import (
    EnvironmentdService,
    EnvironmentdStatefulSet,
    MaterializedAliasService,
)
from materialize.cloudtest.k8s.minio import Minio
from materialize.cloudtest.k8s.persist_pubsub import PersistPubSubService
from materialize.cloudtest.k8s.postgres import postgres_resources
from materialize.cloudtest.k8s.redpanda import redpanda_resources
from materialize.cloudtest.k8s.role_binding import AdminRoleBinding
from materialize.cloudtest.k8s.ssh import ssh_resources
from materialize.cloudtest.k8s.testdrive import Testdrive
from materialize.cloudtest.k8s.vpc_endpoints_cluster_role import VpcEndpointsClusterRole
from materialize.cloudtest.util.wait import wait


class MaterializeApplication(CloudtestApplicationBase):
    def __init__(
        self,
        release_mode: bool = True,
        tag: Optional[str] = None,
        aws_region: Optional[str] = None,
        log_filter: Optional[str] = None,
    ) -> None:
        self.tag = tag
        self.environmentd = EnvironmentdService()
        self.materialized_alias = MaterializedAliasService()
        self.testdrive = Testdrive(release_mode=release_mode, aws_region=aws_region)
        super().__init__(release_mode, aws_region, log_filter)

        # Register the VpcEndpoint CRD.
        self.register_vpc_endpoint()

        self.start_metrics_server()

        self.create_resources_and_wait()

    def get_resources(self, log_filter: Optional[str]) -> List[K8sResource]:
        return [
            *cockroach_resources(),
            *postgres_resources(),
            *redpanda_resources(),
            *debezium_resources(),
            *ssh_resources(),
            Minio(),
            VpcEndpointsClusterRole(),
            AdminRoleBinding(),
            EnvironmentdStatefulSet(
                release_mode=self.release_mode,
                tag=self.tag,
                log_filter=log_filter,
                coverage_mode=self.coverage_mode(),
            ),
            PersistPubSubService(),
            self.environmentd,
            self.materialized_alias,
            self.testdrive,
        ]

    def get_images(self) -> List[str]:
        return ["environmentd", "clusterd", "testdrive", "postgres"]

    def register_vpc_endpoint(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            os.path.join(
                os.path.abspath(self.mz_root),
                "src/cloud-resources/src/crd/gen/vpcendpoints.json",
            ),
        )

    def start_metrics_server(self) -> None:
        self.kubectl(
            "apply",
            "-f",
            "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-3.8.2/components.yaml",
        )
        self.kubectl(
            "patch",
            "deployment",
            "metrics-server",
            "--namespace",
            "kube-system",
            "--type",
            "json",
            "-p",
            '[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls" }]',
        )

    def wait_resource_creation_completed(self) -> None:
        wait(condition="condition=Ready", resource="pod/cluster-u1-replica-u1-0")

    def wait_replicas(self) -> None:
        # NOTE[btv] - This will need to change if the order of
        # creating clusters/replicas changes, but it seemed fine to
        # assume this order, since we already assume it in `create`.
        wait(condition="condition=Ready", resource="pod/cluster-u1-replica-u1-0")
        wait(condition="condition=Ready", resource="pod/cluster-s1-replica-s1-0")
        wait(condition="condition=Ready", resource="pod/cluster-s2-replica-s2-0")

    def wait_for_sql(self) -> None:
        """Wait until environmentd pod is ready and can accept SQL connections"""
        wait(condition="condition=Ready", resource="pod/environmentd-0")

        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=300):
            try:
                self.environmentd.sql("SELECT 1")
                break
            except InterfaceError as e:
                # Since we crash environmentd, we expect some errors that we swallow.
                print(f"SQL interface not ready, {e} while SELECT 1. Waiting...")
                time.sleep(2)

    def set_environmentd_failpoints(self, failpoints: str) -> None:
        """Set the FAILPOINTS environmentd variable in the stateful set. This
        will most likely restart environmentd"""
        stateful_set = [
            resource
            for resource in self.resources
            if type(resource) == EnvironmentdStatefulSet
        ]
        assert len(stateful_set) == 1
        stateful_set = stateful_set[0]

        stateful_set.env["FAILPOINTS"] = failpoints
        stateful_set.replace()
        self.wait_for_sql()
