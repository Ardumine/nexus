using AFCP.Multiplex;
using AFCP.Protocol;
using AFCP.Streams;
using AFCP.Transport;
using System.Net;
using KASerializer;

namespace AFCP;

/// <summary>
/// The server side of an AFCP link. Accepts incoming connections, builds the
/// framed transport stack, runs a <see cref="MultiplexedConnection"/> per peer,
/// and dispatches incoming requests to an <see cref="IAfcpProvider"/>. Subscription
/// pushes flow back over the same connection as <see cref="FrameKind.Notify"/>
/// frames via per-subscription sinks.
/// </summary>
public sealed class AfcpServer : IDisposable
{
    private readonly IAfcpProvider _provider;
    private readonly Serializer _serializer;
    private readonly TcpTransportListener? _listener;
    private readonly List<PeerSession> _sessions = new();
    private readonly object _sessionsLock = new();
    private volatile bool _running;
    private Thread? _acceptThread;

    public AfcpServer(IPEndPoint endpoint, IAfcpProvider provider, Serializer? serializer = null)
    {
        _provider = provider;
        _serializer = serializer ?? new Serializer();
        _listener = new TcpTransportListener(endpoint);
    }

    public void Start()
    {
        if (_running) return;
        _listener!.Start();
        _running = true;
        _acceptThread = new Thread(AcceptLoop) { Name = "AFCP Server Accept", IsBackground = true };
        _acceptThread.Start();
    }

    public void Stop()
    {
        _running = false;
        _listener?.Stop();
        lock (_sessionsLock)
        {
            foreach (var session in _sessions)
            {
                session.Connection.Dispose();
            }
            _sessions.Clear();
        }
    }

    private void AcceptLoop()
    {
        while (_running)
        {
            IConnection? conn;
            try
            {
                conn = _listener!.Accept();
            }
            catch
            {
                break;
            }
            if (conn is null) break;

            var framed = AfcpStack.Build(conn, ownsConnection: true);
            var mux = new MultiplexedConnection(framed, ownsTransport: true);
            var session = new PeerSession(mux, _provider, _serializer);
            mux.Closed += _ =>
            {
                lock (_sessionsLock) { _sessions.Remove(session); }
                // Tear down any subscriptions this peer left open — a client that
                // drops the link without Unsubscribe would otherwise leak the
                // server-side facet subscription (and its consumer loop) forever.
                session.DisposeAllSubscriptions();
            };
            lock (_sessionsLock) { _sessions.Add(session); }
            session.Start();
        }
    }

    public void Dispose() => Stop();

    private sealed class PeerSession : IAfcpSubscriptionSink
    {
        private readonly MultiplexedConnection _connection;
        private readonly IAfcpProvider _provider;
        private readonly Serializer _serializer;
        private readonly Dictionary<ulong, ulong> _activeSubscriptions = new();

        public MultiplexedConnection Connection => _connection;

        public PeerSession(MultiplexedConnection connection, IAfcpProvider provider, Serializer serializer)
        {
            _connection = connection;
            _provider = provider;
            _serializer = serializer;
        }

        public void Start()
        {
            _connection.OnRequest = HandleRequest;
            _connection.Start();
        }

        private byte[]? HandleRequest(ushort messageType, byte[] payload, CancellationToken ct)
        {
            switch (messageType)
            {
                case MessageType.Connect:
                    {
                        var req = Deserialize<ConnectRequest>(payload);
                        var res = _provider.Connect(req);
                        return Serialize(res);
                    }
                case MessageType.Sync:
                    {
                        var req = Deserialize<SyncRequest>(payload);
                        var res = _provider.Sync(req);
                        return Serialize(res);
                    }
                case MessageType.Read:
                    {
                        var req = Deserialize<ReadRequest>(payload);
                        var res = _provider.Read(req);
                        return Serialize(res);
                    }
                case MessageType.Write:
                    {
                        var req = Deserialize<WriteRequest>(payload);
                        var res = _provider.Write(req);
                        return Serialize(res);
                    }
                case MessageType.MkDir:
                    {
                        var req = Deserialize<MkDirRequest>(payload);
                        var res = _provider.MkDir(req);
                        return Serialize(res);
                    }
                case MessageType.Remove:
                    {
                        var req = Deserialize<RemoveRequest>(payload);
                        var res = _provider.Remove(req);
                        return Serialize(res);
                    }
                case MessageType.Subscribe:
                    {
                        var req = Deserialize<SubscribeRequest>(payload);
                        var res = _provider.Subscribe(req, this);
                        if (res.Accepted)
                        {
                            lock (_activeSubscriptions) { _activeSubscriptions[res.SubscriptionId] = res.SubscriptionId; }
                        }
                        return Serialize(res);
                    }
                case MessageType.Unsubscribe:
                    {
                        var req = Deserialize<UnsubscribeRequest>(payload);
                        _provider.Unsubscribe(req.SubscriptionId);
                        lock (_activeSubscriptions) { _activeSubscriptions.Remove(req.SubscriptionId); }
                        return Array.Empty<byte>();
                    }
                case MessageType.Call:
                    {
                        var req = Deserialize<CallRequest>(payload);
                        var res = _provider.Call(req);
                        return Serialize(res);
                    }
                default:
                    return Array.Empty<byte>();
            }
        }

        /// <summary>
        /// Dispose every subscription this peer opened. Called when the connection
        /// closes so the provider can release the backing facet subscriptions.
        /// </summary>
        public void DisposeAllSubscriptions()
        {
            ulong[] ids;
            lock (_activeSubscriptions)
            {
                ids = _activeSubscriptions.Keys.ToArray();
                _activeSubscriptions.Clear();
            }

            foreach (var id in ids)
            {
                try { _provider.Unsubscribe(id); }
                catch { /* teardown is best-effort */ }
            }
        }

        void IAfcpSubscriptionSink.Push(EventNotify evt)
        {
            if (!_connection.IsRunning) return;
            _connection.SendNotify(MessageType.Event, Serialize(evt));
        }

        void IAfcpSubscriptionSink.ProducerGone(ulong subscriptionId)
        {
            if (!_connection.IsRunning) return;
            _connection.SendNotify(MessageType.ProducerGone, Serialize(new ProducerGoneNotify { SubscriptionId = subscriptionId }));
        }

        void IAfcpSubscriptionSink.Error(ulong subscriptionId, string reason)
        {
            if (!_connection.IsRunning) return;
            _connection.SendNotify(MessageType.SubscriptionError, Serialize(new SubscriptionErrorNotify { SubscriptionId = subscriptionId, Reason = reason }));
        }

        private byte[] Serialize<T>(T obj)
        {
            using var ms = new MemoryStream();
            _serializer.Serialize(ms, obj);
            return ms.ToArray();
        }

        private T Deserialize<T>(byte[] data)
        {
            using var ms = new MemoryStream(data);
            return _serializer.Deserialize<T>(ms);
        }
    }
}
