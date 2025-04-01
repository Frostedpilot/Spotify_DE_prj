# Use the same base Airflow version as before
FROM apache/airflow:2.10.5-python3.12

# Set user to root temporarily to install system packages
USER root

# Install OpenJDK 11 (commonly used with Spark 3.x) and procps (for the 'ps' command)
# Use non-interactive frontend to avoid prompts during build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable.
# The exact path might vary slightly depending on the base OS (Debian/Ubuntu)
# but this is a common location for openjdk-11.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Add JAVA_HOME/bin to the PATH for convenience
ENV PATH=$JAVA_HOME/bin:$PATH

# Create the .ivy2 directory and set ownership BEFORE switching user
# This helps ensure correct permissions when the volume is mounted
RUN mkdir -p /home/airflow/.ivy2 && chown -R airflow /home/airflow/.ivy2

# Switch back to the default airflow user
USER airflow

# Install the Python dependencies using ARG/ENV passed from docker-compose
# This keeps the dependency management consistent
ARG _PIP_ADDITIONAL_REQUIREMENTS
RUN if [ -n "$_PIP_ADDITIONAL_REQUIREMENTS" ]; then pip install --no-cache-dir $_PIP_ADDITIONAL_REQUIREMENTS; fi