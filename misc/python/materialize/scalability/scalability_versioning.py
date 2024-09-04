# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from pathlib import Path

from materialize import MZ_ROOT

SCALABILITY_FRAMEWORK_VERSION = "1.3.0"
SCALABILITY_FRAMEWORK_HASH_FILE = Path(__file__).relative_to(MZ_ROOT)

SCALABILITY_FRAMEWORK_DIR = Path(__file__).resolve().parent
SCALABILITY_WORKLOADS_DIR = SCALABILITY_FRAMEWORK_DIR / "workload" / "workloads"

# Consider increasing the #SCALABILITY_FRAMEWORK_VERSION if changes are expected to impact results!
SHA256_OF_FRAMEWORK: dict[str, str] = {}
SHA256_OF_FRAMEWORK["*"] = (
    "1d0fa12d738b686e62d0fa2612656a1b4a0ac43cbfb377f1ffa5c7952ec4314d"
)

# Consider increasing the workload's class #version() if changes are expected to impact results!
SHA256_BY_WORKLOAD_FILE: dict[str, str] = {}
SHA256_BY_WORKLOAD_FILE["connection_workloads.py"] = (
    "025c2df698e38686c66da9ce1afea25b77d2a0059cdcf26a6e831b022133e170"
)
SHA256_BY_WORKLOAD_FILE["ddl_workloads.py"] = (
    "72625e5d50c8a956902a11f139320f0c6b41da0111e488e5a5fe1c4257a7ddc5"
)
SHA256_BY_WORKLOAD_FILE["dml_dql_workloads.py"] = (
    "23f617ab789bed89e880cef7ff884d6b4cc0c2be330ab6916165282f25fbf7e8"
)
SHA256_BY_WORKLOAD_FILE["self_test_workloads.py"] = (
    "471583c9ea29b17ad16566ffcf93b8248e7de204e0a193fa0f08dc46512e96e3"
)
