# Bucket Engine

This memcached engine provides multi-tenancy and isolation between
other memcached engine instances.

It is designed to have minimal overhead while providing few
constraints of its own.

That said, the ACL facilities are currently very limited and thus
require SASL support and two security levels (admin, and everybody
else).

## Building

You will need a storage-engine capable memcached and its included
headers.

The easiest way to do this if you don't want to install memcached from
source would be to just create a source tree and reference it.

### Building Memcached

For example, assume you keep all of your projects in `~/prog/`, you
can do this:

    cd ~/prog
    git clone -b engine git://github.com/dustin/memcached.git
    cd memcached
    ./config/autorun.sh
    ./configure
    make

### Building the Bucket Engine

    cd ~/prog
    git clone git://github.com/northscale/bucket_engine.git
    cd bucket_engine
    ./configure --with-memcached=$HOME/prog/memcached
    make

## Running

An example invocation using the bucket engine from your dev tree
allowing every connecting user to automatically have his own isolated
namespace within the default engine is as follows:

    ~/prog/memcached/memcached -v -S \
        -E ~/prog/bucket_engine/.libs/bucket_engine.so \
        -e engine=$HOME/prog/memcached/.libs/default_engine.so

## Configuration

The following configuration options are available for this engine.

### admin

An administrative user can be specified using the `admin` parameter.

This is the SASL authenticated user permitted to execute
administrative commands (see scripts in the [management][management]
directory for examples).

### auto\_create

With `auto-create` enabled, buckets are created automatically when
users first attempt to use them.

### default

If true, a default bucket exists for unauthenticated users, or users
who don't have buckets created for them when `auto_create` is disabled.

### engine

The contained engine is configured using the `engine` parameter (see
the example above).

[management]: http://github.com/northscale/bucket_engine/tree/master/management/
