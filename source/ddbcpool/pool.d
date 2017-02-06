module ddbcpool.pool;

import core.time;
import std.array;
import std.concurrency;
import std.datetime;

import ddbc;

class ConnectionPool
{
    this(string url, int poolSize, Duration waitTime) shared
    {
        _pool.length = poolSize;
        foreach (i; 0..poolSize) {
            _pool[i] = cast (shared Connection) createConnection(url);
        }

        _waitTime = waitTime;
    }

    void opCall() shared
    {
        auto loop = true;
        while (loop) {
            receive(
                &provideConnection,
                &returnConnection,
                (immutable Terminate _t) {
                    foreach (conn; _pool) {
                        (cast (Connection) conn).close();
                    }

                    loop = false;
                }
            );
        }
    }

    void provideConnection(immutable RequestConnection req)
    {
        auto start = Clock.currTime();
        do {
            if (!_pool.empty) {
                (cast (Tid) req.tid).send(new shared ConnenctionHolder(cast (shared Connection) _pool[0]));
                _pool = _pool[1..$];

                return;
            }
        } while ((Clock.currTime() - start) < _waitTime);

        (cast (Tid) req.tid).send(new immutable ConnectionBusy);
        return;
    }

    void returnConnection(shared ConnenctionHolder holder)
    {
        _pool ~= cast (Connection) holder.conn;
    }

private:
    Connection[] _pool;
    Duration _waitTime;
}



shared class RequestConnection
{
    Tid tid;

    this(shared Tid tid) shared
    {
        this.tid = tid;
    }
}

shared class ConnenctionHolder
{
    Connection conn;
    
    this(shared Connection conn) shared
    {
        this.conn = conn;
    }
}

immutable class ConnectionBusy
{}

immutable class Terminate
{}