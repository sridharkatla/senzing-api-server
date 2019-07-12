ARG BASE_IMAGE=senzing/senzing-base:1.0.3

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM openjdk:8 as builder

ENV REFRESHED_AT=2019-05-01

LABEL Name="senzing/senzing-api-server-builder" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

# Build arguments.

ARG SENZING_G2_JAR_RELATIVE_PATHNAME=unknown
ARG SENZING_G2_JAR_VERSION=unknown

# Install packages via apt.

RUN apt-get update
RUN apt-get -y install \
      make \
      maven \
 && rm -rf /var/lib/apt/lists/*

# Copy the repository from the local host.

COPY . /git-repository

# Run the "make" command to create the artifacts.

WORKDIR /git-repository
RUN export SENZING_API_SERVER_JAR_VERSION=$(mvn "help:evaluate" -Dexpression=project.version -q -DforceStdout); \
    make \
        SENZING_G2_JAR_PATHNAME=/git-repository/${SENZING_G2_JAR_RELATIVE_PATHNAME} \
        SENZING_G2_JAR_VERSION=${SENZING_G2_JAR_VERSION} \
        package; \
    cp /git-repository/target/senzing-api-server-${SENZING_API_SERVER_JAR_VERSION}.jar "/senzing-api-server.jar"

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-05-01

LABEL Name="senzing/senzing-api-server" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      default-jdk \
 && rm -rf /var/lib/apt/lists/*

# Service exposed on port 8080.

EXPOSE 8080

# Copy files from builder step.

COPY --from=builder "/senzing-api-server.jar" "/app/senzing-api-server.jar"

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["/app/docker-entrypoint.sh", "java -jar senzing-api-server.jar" ]
CMD [""]
