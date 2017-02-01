# CEDAR Worker Server

To run the server

    java \
      -Dkeycloak.config.path="$CEDAR_HOME/keycloak.json" \
      -jar $CEDAR_HOME/cedar-worker-server/cedar-worker-server-application/target/cedar-worker-server-application-*.jar \
      server \
      "$CEDAR_HOME/cedar-worker-server/cedar-worker-server-application/config.yml"

To access the application:

[http://localhost:9010/]()

To access the admin port:

[http://localhost:9110/]()