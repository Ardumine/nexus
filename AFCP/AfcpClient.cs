using AFCP.Multiplex;
using AFCP.Protocol;
using AFCP.Streams;
using AFCP.Transport;
using System.Net;
using KASerializer;

namespace AFCP;

/// <summary>
/// The client side of an AFCP link. Connects to a remote <see cref="AfcpServer"/>,
/// runs the handshake, and exposes the path-addressed data-plane verbs
/// (<see cref="SyncAsync"/>, <see cref="ReadAsync"/>, <see cref="SubscribeAsync"/>).
///
/// Subscription pushes arrive as <see cref="FrameKind.Notify"/> frames and are
/// dispatched to the <see cref="IAfcpSubscription"/> handle returned by
/// <see cref="SubscribeAsync"/>. The handle mirrors the local data-plane's
/// mandatory-signal/optional-interruption rule: its <see cref="IAfcpSubscription.State"/>
/// is always observable, the callbacks are optional.
/// </summary>
public sealed class AfcpClient : IDisposable
{
    private readonly Serializer _serializer;
    private MultiplexedConnection? _connection;
    private readonly Dictionary<ulong, ActiveSubscription> _subscriptions = new();
    private readonly object _subscriptionsLock = new();

    public bool IsConnected => _connection?.IsRunning ?? false;

    public AfcpClient(Serializer? serializer = null)
    {
        _serializer = serializer ?? new Serializer();
    }

    /// <summary>The peer name reported by the server during the handshake, if any.</summary>
    public string? RemotePeerName { get; private set; }

    public async Task ConnectAsync(IPEndPoint endpoint, string localPeerName = "client", CancellationToken ct = default)
    {
        var conn = await TcpTransportClient.ConnectAsync(endpoint, ct).ConfigureAwait(false);
        var framed = AfcpStack.Build(conn, ownsConnection: true);
        _connection = new MultiplexedConnection(framed, ownsTransport: true)
        {
            OnNotify = HandleNotify
        };
        _connection.Start();

        var req = new ConnectRequest { ProtocolVersion = ProtocolVersion.Current, PeerName = localPeerName };
        var resBytes = await _connection.SendRequestAsync(MessageType.Connect, Serialize(req), ct).ConfigureAwait(false);
        var res = Deserialize<ConnectResponse>(resBytes);
        if (!res.Accepted)
        {
            _connection.Dispose();
            _connection = null;
            throw new AfcpException($"Server rejected the connection: {res.Error ?? "no reason given"}.");
        }
        RemotePeerName = res.PeerName;
    }

    public Task<SyncResponse> SyncAsync(string pathPrefix, CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new SyncRequest { Path = pathPrefix };
        return RoundTripAsync<SyncRequest, SyncResponse>(MessageType.Sync, req, ct);
    }

    public Task<ReadResponse> ReadAsync(string path, long offset = 0, int maxLength = 0, CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new ReadRequest { Path = path, Offset = offset, MaxLength = maxLength };
        return RoundTripAsync<ReadRequest, ReadResponse>(MessageType.Read, req, ct);
    }

    public Task<WriteResponse> WriteAsync(string path, byte[] data, bool overwrite = true, long offset = 0, CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new WriteRequest { Path = path, Data = data, Overwrite = overwrite, Offset = offset };
        return RoundTripAsync<WriteRequest, WriteResponse>(MessageType.Write, req, ct);
    }

    public Task<MkDirResponse> MkDirAsync(string path, CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new MkDirRequest { Path = path };
        return RoundTripAsync<MkDirRequest, MkDirResponse>(MessageType.MkDir, req, ct);
    }

    public Task<RemoveResponse> RemoveAsync(string path, CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new RemoveRequest { Path = path };
        return RoundTripAsync<RemoveRequest, RemoveResponse>(MessageType.Remove, req, ct);
    }

    /// <summary>
    /// Invoke a method on a remote module instance (Layer 3 — MKCall). The proxy
    /// (<c>RemoteModuleProxy&lt;T&gt;</c> on the mount side) builds the
    /// <see cref="CallRequest"/> from the invoked <see cref="System.Reflection.MethodInfo"/>
    /// and routes it through here. Synchronous from the caller's view: the proxy
    /// blocks on the response (module interface methods are synchronous in V3).
    /// </summary>
    public Task<CallResponse> CallAsync(CallRequest request, CancellationToken ct = default)
    {
        EnsureConnected();
        return RoundTripAsync<CallRequest, CallResponse>(MessageType.Call, request, ct);
    }

    /// <summary>
    /// Subscribe to push events for <paramref name="path"/>. Returns a handle whose
    /// <see cref="IAfcpSubscription.State"/> is always observable; the callbacks are optional.
    /// </summary>
    public async Task<IAfcpSubscription> SubscribeAsync(
        string path,
        Action<EventNotify>? onEvent = null,
        Action? onProducerGone = null,
        Action<string>? onError = null,
        CancellationToken ct = default)
    {
        EnsureConnected();
        var req = new SubscribeRequest { Path = path };
        var res = await RoundTripAsync<SubscribeRequest, SubscribeResponse>(MessageType.Subscribe, req, ct).ConfigureAwait(false);
        if (!res.Accepted)
        {
            throw new AfcpException($"Subscribe to '{path}' was rejected: {res.Error ?? "no reason given"}.");
        }

        var sub = new ActiveSubscription(res.SubscriptionId, onEvent, onProducerGone, onError, res.ValueTypeFullName);
        lock (_subscriptionsLock) { _subscriptions[res.SubscriptionId] = sub; }
        return sub;
    }

    public Task UnsubscribeAsync(IAfcpSubscription subscription, CancellationToken ct = default)
    {
        EnsureConnected();
        if (subscription is not ActiveSubscription active) throw new ArgumentException("Unknown subscription handle.", nameof(subscription));
        lock (_subscriptionsLock) { _subscriptions.Remove(active.SubscriptionId); }
        active.MarkGone(AfcpSubscriptionState.Disposed);
        var req = new UnsubscribeRequest { SubscriptionId = active.SubscriptionId };
        return _connection!.SendRequestAsync(MessageType.Unsubscribe, Serialize(req), ct);
    }

    private void HandleNotify(ushort messageType, byte[] payload)
    {
        switch (messageType)
        {
            case MessageType.Event:
                {
                    var evt = Deserialize<EventNotify>(payload);
                    ActiveSubscription? sub;
                    lock (_subscriptionsLock) { _subscriptions.TryGetValue(evt.SubscriptionId, out sub); }
                    sub?.OnEvent?.Invoke(evt);
                    break;
                }
            case MessageType.ProducerGone:
                {
                    var gone = Deserialize<ProducerGoneNotify>(payload);
                    ActiveSubscription? sub;
                    lock (_subscriptionsLock)
                    {
                        _subscriptions.TryGetValue(gone.SubscriptionId, out sub);
                        _subscriptions.Remove(gone.SubscriptionId);
                    }
                    if (sub is not null)
                    {
                        sub.MarkGone(AfcpSubscriptionState.ProducerGone);
                        sub.OnProducerGone?.Invoke();
                    }
                    break;
                }
            case MessageType.SubscriptionError:
                {
                    var err = Deserialize<SubscriptionErrorNotify>(payload);
                    ActiveSubscription? sub;
                    lock (_subscriptionsLock)
                    {
                        _subscriptions.TryGetValue(err.SubscriptionId, out sub);
                        _subscriptions.Remove(err.SubscriptionId);
                    }
                    if (sub is not null)
                    {
                        sub.MarkGone(AfcpSubscriptionState.Errored);
                        sub.OnError?.Invoke(err.Reason ?? "unknown");
                    }
                    break;
                }
        }
    }

    private async Task<TResponse> RoundTripAsync<TRequest, TResponse>(ushort messageType, TRequest request, CancellationToken ct)
        where TResponse : class
    {
        var payload = Serialize(request);
        var responseBytes = await _connection!.SendRequestAsync(messageType, payload, ct).ConfigureAwait(false);
        return Deserialize<TResponse>(responseBytes);
    }

    private void EnsureConnected()
    {
        if (_connection is null || !_connection.IsRunning)
        {
            throw new InvalidOperationException("AfcpClient is not connected.");
        }
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

    public void Dispose()
    {
        lock (_subscriptionsLock)
        {
            foreach (var sub in _subscriptions.Values)
            {
                sub.MarkGone(AfcpSubscriptionState.Disposed);
            }
            _subscriptions.Clear();
        }
        _connection?.Dispose();
    }

    private sealed class ActiveSubscription : IAfcpSubscription
    {
        public ActiveSubscription(ulong subscriptionId, Action<EventNotify>? onEvent, Action? onProducerGone, Action<string>? onError, string? valueTypeFullName)
        {
            SubscriptionId = subscriptionId;
            OnEvent = onEvent;
            OnProducerGone = onProducerGone;
            OnError = onError;
            ValueTypeFullName = valueTypeFullName;
        }

        public ulong SubscriptionId { get; }
        public Action<EventNotify>? OnEvent { get; }
        public Action? OnProducerGone { get; }
        public Action<string>? OnError { get; }
        public string? ValueTypeFullName { get; }
        public AfcpSubscriptionState State { get; private set; } = AfcpSubscriptionState.Active;

        public void MarkGone(AfcpSubscriptionState state)
        {
            State = state;
        }
    }
}

/// <summary>Protocol version advertised in the <see cref="ConnectRequest"/> handshake.</summary>
public static class ProtocolVersion
{
    public const ushort Current = 1;
}

public enum AfcpSubscriptionState
{
    Active,
    ProducerGone,
    Errored,
    Disposed,
}

/// <summary>
/// A handle to an active remote subscription. The <see cref="State"/> is always
/// observable (the mandatory signal); the callbacks passed to
/// <see cref="AfcpClient.SubscribeAsync"/> are the optional interruption.
/// </summary>
public interface IAfcpSubscription
{
    ulong SubscriptionId { get; }
    AfcpSubscriptionState State { get; }
    string? ValueTypeFullName { get; }
}

public class AfcpException : Exception
{
    public AfcpException(string message) : base(message) { }
}
