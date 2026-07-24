# Security policy

## Reporting a vulnerability

Report security issues privately. Please don't open a public issue. Use GitHub's
**Report a vulnerability** button under the Security tab, or email 1fannnw@gmail.com.

Expect an acknowledgement within a few days and updates as a fix is worked out.

## Scope

This is a third-party Apache Airflow provider. It evaluates feature flags through OpenFeature and
applies cluster policies; it stores no secrets. Backend credentials live in your OpenFeature provider
configuration or Airflow connections, not in this package. Installing the package is a no-op until the
policy or listener is enabled in config.
