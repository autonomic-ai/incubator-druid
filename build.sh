./mvnw versions:set -DnewVersion="$FINAL_OR_BRANCH_SNAPSHOT_VERSION"
./mvnw -DskipTests=true -Pdist deploy

if [ -z $FINAL_OR_BRANCH_SNAPSHOT_VERSION ]; then
    sed "s%<DRUID_VERSION>%0.13.1-incubating-SNAPSHOT%g" Dockerfile.template > Dockerfile
else
    sed "s%<DRUID_VERSION>%0.13.0-incubating-au-$FINAL_OR_BRANCH_SNAPSHOT_VERSION%g" Dockerfile.template > Dockerfile
fi
