module ddbcpool.provider;

import std.concurrency;
import core.time;

import ddbc;

import ddbcpool.pool;

class ConnectionProvider
{
    this(string url, int poolSize = 10, int waitSeconds = 5) shared
    {
        _pool = cast (shared Tid) spawn(new shared ConnectionPool(url, poolSize, waitSeconds.dur!"seconds"));
        _waitSeconds = waitSeconds;
    }

    ~this() shared
    {
        (cast (Tid) _pool).send(new immutable Terminate);
    }

    Connection requestConnection() shared
    {
        (cast (Tid) _pool).send(new shared RequestConnection(cast (shared Tid) thisTid));
        Connection ret;

        receiveTimeout(
            (_waitSeconds * 2).dur!"seconds",
            (shared ConnenctionHolder holder) {
                ret = cast (Connection) holder.conn;
            },
            (immutable ConnectionBusy _m) {
                ret = null;
            }
        );

        return ret;
    }

    void releaseConnection(ref Connection conn) shared
    {
        auto holder = new shared ConnenctionHolder(cast (shared Connection) conn);
        (cast (Tid) _pool).send(holder);
        conn = null;

        return;
    }

private:
    Tid _pool;
    int _waitSeconds;
}