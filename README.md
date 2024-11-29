```
[]++++||=======>
```

# Baselard

Baselard is a library for building and executing directed acyclic graphs (DAGs) of components.

## Features:

- [x] Define a DAG in JSON
- [x] Execute a DAG from an intermediate representation
- [x] Validate configuration of components
- [x] Handle errors in components
- [x] Time out components
- [x] Register custom components
- [x] Type guarantees on inputs and outputs of components
- [x] But also... allow "wildcard" inputs and outputs in components via JSON
- [x] Types include `Union` types to allow flexibility
- [x] Cache results of components
- [x] Keep a request history of components for replay
- [x] Automatically handle parallel nodes
- [x] JQ transformations
- [ ] Streaming will not be supported: it doesn't match the scheduling mechanism

To try a complex transform (if you have `cargo run --example serving` running),

```shell
curl -s -X POST http://localhost:3000/execute \
  -H "Content-Type: application/json" \
  -d @tests/resources/complex_transform.json
```

You can also turn the cache off by adding `"cache": "no-cache"` to the request.

```shell
curl -s -X POST http://localhost:3000/execute \
  -H "Cache-Control: no-cache" \
  -H "Content-Type: application/json" \
  -d @tests/resources/timestamp_bucketing.json
```

### JQ Setup

The `jq-rs` crate has bindings specifically for version 1.6 of `libjq` ([not 1.7](https://github.com/onelson/jq-rs/issues/37)).

We install `libjq` manually from source as relying on the `bundled` flag causes errors when building on macOS. Note that if you are running on AL2, the `bundled` flag works properly, but we will still opt to install via source.

#### macOS

Install these system dependencies:

```shell
brew install flex
brew install bison
brew install libtool
brew install automake
```

Export these environment variables:

1. We are adding `bison` to the PATH because `bison` is "keg-only" when installed via Homebrew and not automatically added to the PATH.
2. We are setting `CPPFLAGS` to `-D_REENTRANT` as suggested in this [issue](https://github.com/jqlang/jq/issues/1936#issuecomment-1329730074) to fix the `error: call to undeclared function 'lgamma_r'; ISO C99 and later do not support implicit function declarations [-Wimplicit-function-declaration]` error.

```shell
export PATH="/opt/homebrew/opt/bison/bin:$PATH"
export CPPFLAGS=-D_REENTRANT
```

Now you can proceed with installing `libjq` from source as shown in the "Install `libjq` from source" section below.

#### Amazon Linux 2

On AL2 (Amazon Linux 2), you may run into the following error when trying to install `jq-rs` (both manualy from source or via the `bundled` feature flag):

```shell
❯ autoreconf
configure.ac:6: error: require Automake 1.14, but have 1.13.4
autoreconf: automake failed with exit status: 1
```

To fix this, we can install version libtool 2.4.2 and automake 1.14.

First uninstall any existing versions of automake and libtool. On AL2, this is done like so:

```shell
sudo yum remove -y automake libtool
```

To install libtool 2.4.2 from source, run the following commands:

```shell
wget https://ftp.gnu.org/gnu/libtool/libtool-2.4.2.tar.gz
tar -xvf libtool-2.4.2.tar.gz
cd libtool-2.4.2
./configure
make
sudo make install
cd ../
rm -rf libtool-2.4.2
rm libtool-2.4.2.tar.gz
```

Verify the installation like so:

```shell
❯ libtool --version | head -n1
libtool (GNU libtool) 2.4.2
```

To install automake 1.14 from source, run the following commands:

```shell
wget https://ftp.gnu.org/gnu/automake/automake-1.14.tar.gz
tar -xvf automake-1.14.tar.gz
cd automake-1.14
./configure
make
sudo make install
cd ../
rm -rf automake-1.14
rm automake-1.14.tar.gz
```

Verify the installation like so:

```shell
❯ automake --version | head -n1
automake (GNU automake) 1.14
```

Finally uninstall any existing versions of libjq:

```shell
sudo yum remove -y jq-devel
```

Finally, set this environment variable to build libjq with the position independent code flag:

```shell
export CFLAGS=-fPIC
```

Without this flag, you will get a bunch of errors that look like this:

```
/usr/bin/ld: /.../baselard/target/debug/deps/libjq_sys-68c04a110255c875.rlib(unicode_fold3_key.o): relocation R_X86_64_32S against `.rodata' can not be used when making a shared object; recompile with -fPIC
```

Now you can proceed with installing `libjq` from source as shown in the "Install `libjq` from source" section below.

#### Install `libjq` from source

This is the shared logic to install `libjq` from source on both macOS and AL2. Please ensure you have set the platform-specific environment variables shown above in the macOS or AL2 sections.

```shell
JQ_VERSION=1.6
wget https://github.com/jqlang/jq/releases/download/jq-$JQ_VERSION/jq-$JQ_VERSION.tar.gz
tar -xvf jq-$JQ_VERSION.tar.gz
cd jq-$JQ_VERSION
autoreconf -fi
./configure --with-oniguruma=builtin
make -j8
make check
sudo make install
cd ../
rm -rf jq-$JQ_VERSION
rm jq-$JQ_VERSION.tar.gz
```
