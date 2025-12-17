# fabric-dbt-runner

dbt runner allows you to execute dbt models natively inside Microsoft Fabric using:

Fabric Notebooks

Lakehouse for dbt project metadata

Variable Libraries for profile/workspace cicd config

No external runners. No containers. No workarounds.

Why dbt runner exists

Running dbt in Fabric today usually means:

External CI agents

Custom containers

Secrets scattered across pipelines

Hard OTAP separation

dbt runner turns Fabric itself into the dbt runtime.

What it does

Executes dbt models from a Fabric Notebook

Uses Variable Libraries for:

targets

schemas

dbt selector

environment switching (OTAP)

Stores dbt metadata in a Fabric Lakehouse

Designed for CI/CD and enterprise governance

<img width="3324" height="1924" alt="image" src="https://github.com/user-attachments/assets/b02c402b-ed2b-4f96-8f05-c3a62fa4d31a" />
