# Workflow Runner Context

A **compiled workflow** is an execution-neutral, immutable DAG produced by `workflow-editor`.

A **run** is one invocation of one compiled workflow. A **node attempt** is one attempt to execute a node during that run.

A **node executor** implements one node `kind`. The runner contains only small built-in executors for the editor's basic control-flow and JSON primitive nodes; domain-specific behavior is registered by hosts.

Missing output ports are inactive. If all incoming edges to a node are inactive, the node is skipped. This is the MVP branch-propagation rule used by `control.if` and `control.switch`.
