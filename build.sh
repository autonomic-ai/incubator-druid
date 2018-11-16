./mvnw versions:set -DnewVersion="0.12.3-au-$FINAL_OR_BRANCH_SNAPSHOT_VERSION"
./mvnw -DskipTests=true install

sed "s%<DRUID_VERSION>%0.12.3-au-$FINAL_OR_BRANCH_SNAPSHOT_VERSION%g" Dockerfile.template > Dockerfile
