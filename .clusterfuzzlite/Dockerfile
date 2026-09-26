FROM gcr.io/oss-fuzz-base/base-builder-rust
# libsqlite3-sys's build script runs bindgen for the session extension, which needs libclang
RUN apt-get update && apt-get install -y --no-install-recommends libclang-dev && rm -rf /var/lib/apt/lists/*
COPY . $SRC/subql
WORKDIR $SRC/subql
COPY ./.clusterfuzzlite/build.sh $SRC/
