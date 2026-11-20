#!/usr/bin/env bash
# Builds the PgOutput2Json packages (the core library and every output) in Release, at the version in src/Directory.Build.props,
# and pushes them to nuget.org. Prompts for the API key; it's never written to disk. The symbols (.snupkg) next to each package
# are pushed with it.
set -euo pipefail

cd "$(dirname "$0")"

version=$(sed -n 's|.*<Version>\(.*\)</Version>.*|\1|p' src/Directory.Build.props)
[[ -n "$version" ]] || { echo "No <Version> in src/Directory.Build.props" >&2; exit 1; }

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "The working tree has changes: commit or stash them first, so the packages match a commit." >&2
    exit 1
fi

tag="v$version"
if [[ "$(git rev-parse -q --verify "$tag^{commit}" || true)" != "$(git rev-parse HEAD)" ]]; then
    echo "Warning: HEAD isn't tagged $tag."
    read -rp "Publish $version anyway? [y/N] " answer
    [[ "$answer" == [yY] ]] || exit 1
fi

# the core library first: the outputs depend on it at the same version
projects=(
    PgOutput2Json
    PgOutput2Json.AzureEventHubs
    PgOutput2Json.ClickHouse
    PgOutput2Json.DynamoDb
    PgOutput2Json.Kafka
    PgOutput2Json.Kinesis
    PgOutput2Json.MongoDb
    PgOutput2Json.RabbitMq
    PgOutput2Json.RabbitMqStreams
    PgOutput2Json.Redis
    PgOutput2Json.Sqlite
    PgOutput2Json.Webhooks
)

packages=()
for project in "${projects[@]}"; do
    packages+=("src/$project/bin/Release/$project.$version.nupkg")
done

# GeneratePackageOnBuild: the build creates the packages (dotnet pack wouldn't build). Old ones are removed first, so a failed
# build can't leave a stale package to push
rm -f "${packages[@]}" "${packages[@]/%.nupkg/.snupkg}"

for project in "${projects[@]}"; do
    dotnet build -c Release --disable-build-servers "src/$project/$project.csproj"
done

for package in "${packages[@]}"; do
    [[ -f "$package" ]] || { echo "Missing $package" >&2; exit 1; }
done

read -rsp "nuget.org API key for $version: " api_key
echo
[[ -n "$api_key" ]] || { echo "No API key: nothing pushed." >&2; exit 1; }

for package in "${packages[@]}"; do
    dotnet nuget push "$package" --api-key "$api_key" --source https://api.nuget.org/v3/index.json --skip-duplicate
done

unset api_key

echo "Pushed ${#packages[@]} PgOutput2Json packages, $version."
