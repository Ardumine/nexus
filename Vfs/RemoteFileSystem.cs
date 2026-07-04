using AFCP;
using AFCP.Protocol;
using HCore.Modules.Base;
using KASerializer;
using System.Text;
using System.Threading.Channels;

namespace HCore.Packages.Nexus;

/// <summary>
/// A read-write <see cref="IVirtualFileSystem"/> backed by a remote AFCP peer.
/// Serves the peer's entire root tree (not just <c>/proc</c>): the peer's
/// <c>VfsAfcpProvider</c> proxies its kernel <see cref="FileSystem"/>, which
/// mounts <c>/proc</c>, <c>/etc</c>, <c>/dev</c>, etc. No capability model exists
/// yet (TODO.md §C3) — any mounting peer can write anywhere under the served
/// root, same trusted-LAN gap as <c>Kill</c>.
///
/// The tree is **lazy** (9P-style): each <see cref="RemoteDirectory"/> fetches its
/// entries via a fresh <see cref="AfcpClient.SyncAsync"/> on access, and each
/// <see cref="RemoteFile"/> fetches its bytes via <see cref="AfcpClient.ReadAsync"/>
/// on read — so <c>ls</c> walks into one directory at a time and <c>cat</c> always
/// sees the latest content (a live <c>/proc</c> facet is re-read on every access).
/// Nothing is cached. Writes (<see cref="AfcpClient.WriteAsync"/>,
/// <see cref="AfcpClient.MkDirAsync"/>, <see cref="AfcpClient.RemoveAsync"/>) are
/// whole-file, single round-trip operations — no chunked/streaming write (see
/// TODO.md §C7e).
/// </summary>
internal sealed class RemoteFileSystem : IVirtualFileSystem, IDisposable, IRemoteDataSource
{
    private readonly AfcpClient _client;
    private readonly Serializer _serializer;

    /// <summary>
    /// Wire chunk size for streamed reads/writes (§C7e). Kept well under the
    /// transport's 64 MiB frame cap (<c>FramedTransport.MaxMessageBytes</c>) so a
    /// single file never rides in one oversized frame; large files stream as a
    /// sequence of these.
    /// </summary>
    internal const int ChunkBytes = 1 * 1024 * 1024;

    public string Name => "afcp-remote";
    public bool IsReadOnly => false;
    public string RemoteEndpoint { get; }

    /// <summary>
    /// The client backing this mount — exposed (internal) so the MKCall proxy
    /// (<see cref="RemoteModuleProxy{T}"/>) can round-trip <c>Call</c> requests
    /// through the same connection that already serves this mount's VFS + data
    /// traffic. One peer, one connection, three layers.
    /// </summary>
    internal AfcpClient Client => _client;

    public RemoteFileSystem(AfcpClient client, string mountPoint, Serializer serializer)
    {
        _client = client;
        _serializer = serializer;
        RemoteEndpoint = mountPoint;
    }

    public IVirtualDirectory Root => new RemoteDirectory("/", null, _client, "/");

    public ISubscription SubscribeData<T>(
        string remotePath,
        Func<DataEvent<T>, CancellationToken, ValueTask> handler,
        Action<DisconnectReason>? onDisconnected) where T : class
        => RemoteSubscription<T>.Start(_client, _serializer, remotePath, handler, onDisconnected);

    public void Dispose() => _client.Dispose();
}

/// <summary>
/// Maps a wire-level <see cref="AfcpErrorCode"/> (TODO.md §C7d) back to the .NET
/// exception a local VFS would have thrown, so remote failures are indistinguishable
/// from local ones to user space: a missing remote file throws
/// <see cref="FileNotFoundException"/>, a permission refusal throws
/// <see cref="UnauthorizedAccessException"/>, and a type mismatch (file vs directory)
/// throws <see cref="IOException"/>.
/// </summary>
internal static class RemoteError
{
    public static Exception ToException(AfcpErrorCode code, string? message, string path) => code switch
    {
        AfcpErrorCode.NotFound => new FileNotFoundException(message ?? $"'{path}' not found on the remote peer.", path),
        AfcpErrorCode.NotADirectory => new IOException(message ?? $"'{path}' is not a directory on the remote peer."),
        AfcpErrorCode.NotAFile => new IOException(message ?? $"'{path}' is not a file on the remote peer."),
        AfcpErrorCode.PermissionDenied => new UnauthorizedAccessException(message ?? $"Access to '{path}' was denied by the remote peer."),
        AfcpErrorCode.ReadOnly => new IOException(message ?? $"'{path}' is on a read-only remote filesystem."),
        _ => new IOException(message ?? $"Remote operation on '{path}' failed."),
    };
}

/// <summary>
/// A lazy directory backed by a remote AFCP <see cref="SyncAsync"/>. Every
/// enumeration/lookup does a fresh round-trip so newly-spawned remote instances
/// appear immediately — matching <see cref="ProcFileSystem"/>'s live-view model.
/// </summary>
internal sealed class RemoteDirectory : VirtualNode, IVirtualDirectory
{
    private readonly AfcpClient _client;
    private readonly string _remotePath;

    public RemoteDirectory(string name, IVirtualDirectory? parent, AfcpClient client, string remotePath)
        : base(name, parent)
    {
        _client = client;
        _remotePath = remotePath;
    }

    private DirEntry[] Fetch()
    {
        var res = _client.SyncAsync(_remotePath).GetAwaiter().GetResult();
        if (res.Error == AfcpErrorCode.NotFound)
        {
            // A missing directory looks empty — preserves kernel path traversal
            // (TryGet* returns null on an absent child rather than throwing), the
            // same as the pre-C7d empty-listing behaviour. Harder errors below.
            return Array.Empty<DirEntry>();
        }
        if (res.Error != AfcpErrorCode.None)
        {
            throw RemoteError.ToException(res.Error, res.ErrorMessage, Path);
        }
        return res.Entries;
    }

    private static string ChildRemotePath(string parentRemote, string name)
        => parentRemote == "/" ? $"/{name}" : $"{parentRemote}/{name}";

    public IEnumerable<IVirtualNode> Enumerate()
    {
        return Fetch().Select(ToNode);
    }

    public IEnumerable<IVirtualNode> EnumerateDirectories()
        => Enumerate().Where(n => n is IVirtualDirectory);

    public IEnumerable<IVirtualNode> EnumerateFiles()
        => Enumerate().Where(n => n is IVirtualFile);

    public IVirtualDirectory? TryGetDirectory(string name)
        => Fetch().FirstOrDefault(e => e.IsDirectory && string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase)) is { } entry
            ? (IVirtualDirectory)ToNode(entry)
            : null;

    public IVirtualDirectory GetDirectory(string name)
        => TryGetDirectory(name) ?? throw new DirectoryNotFoundException($"Directory '{name}' not found in '{Path}'.");

    public IVirtualFile? TryGetFile(string name)
        => Fetch().FirstOrDefault(e => !e.IsDirectory && string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase)) is { } entry
            ? (IVirtualFile)ToNode(entry)
            : null;

    public IVirtualFile GetFile(string name)
        => TryGetFile(name) ?? throw new FileNotFoundException($"File '{name}' not found in '{Path}'.");

    public IVirtualNode? TryGet(string name)
        => Fetch().FirstOrDefault(e => string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase)) is { } entry
            ? ToNode(entry)
            : null;

    public IVirtualDirectory CreateDirectory(string name)
    {
        var childRemote = ChildRemotePath(_remotePath, name);
        var res = _client.MkDirAsync(childRemote).GetAwaiter().GetResult();
        if (!res.Success)
        {
            throw new IOException(res.Error ?? $"Failed to create directory '{childRemote}' on the remote peer.");
        }

        return new RemoteDirectory(name, this, _client, childRemote);
    }

    public bool TryDelete(string name)
    {
        var childRemote = ChildRemotePath(_remotePath, name);
        var res = _client.RemoveAsync(childRemote).GetAwaiter().GetResult();
        return res.Success;
    }

    public IVirtualFile CreateFile(string name, bool overwrite = true, ReadOnlySpan<byte> initialData = default)
    {
        var childRemote = ChildRemotePath(_remotePath, name);
        RemoteFile.WriteChunked(_client, childRemote, initialData.ToArray(), overwrite);
        return new RemoteFile(name, this, _client, childRemote);
    }

    private IVirtualNode ToNode(DirEntry entry)
    {
        var childRemote = ChildRemotePath(_remotePath, entry.Name);
        return entry.IsDirectory
            ? new RemoteDirectory(entry.Name, this, _client, childRemote)
            : new RemoteFile(entry.Name, this, _client, childRemote);
    }
}

/// <summary>
/// A file whose content is fetched from the remote AFCP peer on every read via
/// <see cref="AfcpClient.ReadAsync"/> — never cached, so <c>cat</c> on a live
/// <c>/proc</c> facet always reflects the latest frame. Writes
/// (<see cref="Write"/>, or a write-access <see cref="GetStream"/>) are buffered
/// locally and sent as a single whole-file <see cref="AfcpClient.WriteAsync"/>
/// round-trip — AFCP has no chunked/streaming write (TODO.md §C7e).
/// </summary>
internal sealed class RemoteFile : VirtualNode, IVirtualFile
{
    private readonly AfcpClient _client;
    private readonly string _remotePath;

    public RemoteFile(string name, IVirtualDirectory parent, AfcpClient client, string remotePath)
        : base(name, parent)
    {
        _client = client;
        _remotePath = remotePath;
    }

    private byte[] FetchAll()
    {
        using var stream = new RemoteReadStream(_client, _remotePath);
        using var ms = new MemoryStream(stream.Length > 0 && stream.Length <= int.MaxValue ? (int)stream.Length : 0);
        stream.CopyTo(ms, RemoteFileSystem.ChunkBytes);
        return ms.ToArray();
    }

    public Stream GetStream(FileMode mode = FileMode.Open, FileAccess access = FileAccess.Read)
    {
        if (access == FileAccess.Read)
        {
            // Lazy, seekable, chunk-fetching read stream (§C7e): bounded memory,
            // one round-trip per ChunkBytes window, so big files never ride one frame.
            return new RemoteReadStream(_client, _remotePath);
        }

        // Append and Open(OrCreate) preserve existing remote content (seeded
        // then overwritten from the start, mirroring a real file handle);
        // Create/CreateNew/Truncate start from an empty buffer. Either way the
        // whole buffer is sent back as chunked Writes on Dispose.
        byte[] initial = Array.Empty<byte>();
        if (mode is FileMode.Append or FileMode.Open or FileMode.OpenOrCreate)
        {
            try { initial = FetchAll(); }
            catch (FileNotFoundException) { /* nothing to seed; start empty */ }
        }

        var stream = new RemoteWriteStream(_client, _remotePath, initial);
        if (mode != FileMode.Append)
        {
            stream.Position = 0;
        }

        return stream;
    }

    public byte[] ReadAllBytes() => FetchAll();

    public string ReadString(Encoding? encoding = null)
    {
        encoding ??= Encoding.UTF8;
        return encoding.GetString(FetchAll());
    }

    public void Write(ReadOnlySpan<byte> data)
    {
        WriteChunked(_client, _remotePath, data.ToArray(), overwrite: true);
    }

    /// <summary>
    /// Send <paramref name="data"/> to the remote peer as a sequence of
    /// <see cref="RemoteFileSystem.ChunkBytes"/>-sized <c>Write</c> requests (§C7e):
    /// the first chunk creates/overwrites from offset 0 (honouring
    /// <paramref name="overwrite"/>), each subsequent chunk seeks + writes at its
    /// offset. A file that fits in one chunk is a single round-trip (unchanged from
    /// the pre-C7e behaviour). Empty data still sends one chunk, creating an empty file.
    /// </summary>
    internal static void WriteChunked(AfcpClient client, string remotePath, byte[] data, bool overwrite)
    {
        long offset = 0;
        do
        {
            var len = (int)Math.Min(RemoteFileSystem.ChunkBytes, data.Length - offset);
            var chunk = new byte[len];
            Array.Copy(data, offset, chunk, 0, len);

            // Only the first chunk honours Overwrite; continuation chunks (offset > 0)
            // are seek+write on the server regardless.
            var res = client
                .WriteAsync(remotePath, chunk, overwrite: offset == 0 ? overwrite : true, offset: offset)
                .GetAwaiter().GetResult();
            if (!res.Success)
            {
                throw new IOException(res.Error ?? $"Failed to write '{remotePath}' on the remote peer.");
            }

            offset += len;
        }
        while (offset < data.Length);
    }

    /// <summary>
    /// A <see cref="MemoryStream"/> that flushes its full contents to the remote
    /// peer as chunked <see cref="AfcpClient.WriteAsync"/> calls on
    /// <see cref="Dispose(bool)"/> (§C7e) — the mount-side half of AFCP's
    /// <c>Write</c> verb. Not safe to reuse after disposal (matches
    /// <see cref="MemoryStream"/>'s own contract).
    /// </summary>
    private sealed class RemoteWriteStream : MemoryStream
    {
        private readonly AfcpClient _client;
        private readonly string _remotePath;

        public RemoteWriteStream(AfcpClient client, string remotePath, byte[] initialContent)
        {
            _client = client;
            _remotePath = remotePath;
            if (initialContent.Length > 0)
            {
                Write(initialContent, 0, initialContent.Length);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                WriteChunked(_client, _remotePath, ToArray(), overwrite: true);
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// A read-only, seekable <see cref="Stream"/> backed by chunked AFCP
    /// <c>Read</c> requests (§C7e). Fetches one <see cref="RemoteFileSystem.ChunkBytes"/>
    /// window at a time from the peer and caches only that window, so memory stays
    /// bounded regardless of file size and no single frame exceeds the transport cap.
    /// The constructor primes the first window, which also surfaces not-found /
    /// permission errors eagerly (like opening a local file) and learns the total
    /// <see cref="Length"/>.
    /// </summary>
    private sealed class RemoteReadStream : Stream
    {
        private readonly AfcpClient _client;
        private readonly string _remotePath;
        private long _position;
        private long _length;
        private byte[] _chunk = Array.Empty<byte>();
        private long _chunkStart;

        public RemoteReadStream(AfcpClient client, string remotePath)
        {
            _client = client;
            _remotePath = remotePath;
            FetchChunk(0);
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => _length;

        public override long Position
        {
            get => _position;
            set => _position = value < 0 ? throw new IOException("Position cannot be negative.") : value;
        }

        private void FetchChunk(long start)
        {
            var res = _client.ReadAsync(_remotePath, start, RemoteFileSystem.ChunkBytes).GetAwaiter().GetResult();
            if (res.Error != AfcpErrorCode.None)
            {
                throw RemoteError.ToException(res.Error, res.ErrorMessage, _remotePath);
            }
            _length = res.TotalLength;
            _chunk = res.Data ?? Array.Empty<byte>();
            _chunkStart = start;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position >= _length || count == 0) return 0;

            if (_position < _chunkStart || _position >= _chunkStart + _chunk.Length)
            {
                FetchChunk(_position);
                if (_chunk.Length == 0) return 0;
            }

            var chunkOffset = (int)(_position - _chunkStart);
            var available = _chunk.Length - chunkOffset;
            var n = Math.Min(count, available);
            Array.Copy(_chunk, chunkOffset, buffer, offset, n);
            _position += n;
            return n;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var target = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End => _length + offset,
                _ => _position,
            };
            if (target < 0) throw new IOException("Cannot seek before the start of the stream.");
            _position = target;
            return _position;
        }

        public override void Flush() { }
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("Remote read stream is read-only.");
    }
}

/// <summary>
/// Mount-side <see cref="ISubscription"/> that wraps a remote AFCP subscription and
/// makes it look like a local one. AFCP notify frames are dispatched on thread-pool
/// threads (see <c>MultiplexedConnection</c>), so this adapter funnels them through a
/// bounded <see cref="Channel{T}"/> + a single consumer loop — the user handler is
/// therefore invoked by exactly one thread at a time, matching the local
/// single-consumer contract. Wire-order vs enqueue-order under concurrent dispatch
/// stays observable through <see cref="DataEvent{T}.Sequence"/>, the same as local
/// overflow gaps.
/// </summary>
internal sealed class RemoteSubscription<T> : ISubscription where T : class
{
    // Stream default (DATA_PLANE_DECISIONS.md B3): bounded 64, drop-oldest.
    private const int ChannelBound = 64;

    private readonly AfcpClient _client;
    private readonly Serializer _serializer;
    private readonly Func<DataEvent<T>, CancellationToken, ValueTask> _handler;
    private readonly Action<DisconnectReason>? _onDisconnected;
    private readonly Channel<DataEvent<T>> _channel;
    private readonly CancellationTokenSource _cts = new();

    private IAfcpSubscription? _remote;
    private int _state = (int)SubscriptionState.Active;
    private DisconnectReason? _disconnectReason;
    private long _consumerSkippedCount;

    public SubscriptionState State => (SubscriptionState)Volatile.Read(ref _state);
    public DisconnectReason? DisconnectReason => _disconnectReason;
    public long ConsumerSkippedCount => Interlocked.Read(ref _consumerSkippedCount);

    private RemoteSubscription(
        AfcpClient client,
        Serializer serializer,
        Func<DataEvent<T>, CancellationToken, ValueTask> handler,
        Action<DisconnectReason>? onDisconnected)
    {
        _client = client;
        _serializer = serializer;
        _handler = handler;
        _onDisconnected = onDisconnected;
        _channel = Channel.CreateBounded<DataEvent<T>>(new BoundedChannelOptions(ChannelBound)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
    }

    public static RemoteSubscription<T> Start(
        AfcpClient client,
        Serializer serializer,
        string remotePath,
        Func<DataEvent<T>, CancellationToken, ValueTask> handler,
        Action<DisconnectReason>? onDisconnected)
    {
        var sub = new RemoteSubscription<T>(client, serializer, handler, onDisconnected);
        _ = Task.Run(sub.ConsumeAsync);

        // SubscribeAsync throws (AfcpException) if the peer rejects the facet;
        // let that propagate to the caller, matching the local "No data facet" throw.
        sub._remote = client.SubscribeAsync(
            remotePath,
            onEvent: sub.OnRemoteEvent,
            onProducerGone: () => sub.Trip(HCore.Modules.Base.DisconnectReason.ProducerKilled),
            onError: reason => sub.Trip(MapError(reason)))
            .GetAwaiter().GetResult();

        return sub;
    }

    private void OnRemoteEvent(EventNotify evt)
    {
        if (State != SubscriptionState.Active) return;

        T value;
        using (var ms = new MemoryStream(evt.Data ?? Array.Empty<byte>()))
        {
            value = _serializer.Deserialize<T>(ms);
        }

        var frame = new DataEvent<T>
        {
            Data = value,
            Sequence = evt.Sequence,
            InterFrameDelta = evt.HasInterFrameDelta ? evt.InterFrameDelta : null,
        };

        // Bounded, drop-oldest: a slow local handler drops the oldest queued frame
        // rather than blocking the notify thread — Sequence gaps stay observable.
        _channel.Writer.TryWrite(frame);
    }

    private async Task ConsumeAsync()
    {
        try
        {
            await foreach (var frame in _channel.Reader.ReadAllAsync(_cts.Token).ConfigureAwait(false))
            {
                try
                {
                    await _handler(frame, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    Interlocked.Increment(ref _consumerSkippedCount);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposed / tripped — normal shutdown.
        }
    }

    private void Trip(DisconnectReason reason)
    {
        if (Interlocked.CompareExchange(ref _state, (int)SubscriptionState.Tripped, (int)SubscriptionState.Active)
            != (int)SubscriptionState.Active)
        {
            return;
        }

        _disconnectReason = reason;
        _cts.Cancel();
        _channel.Writer.TryComplete();

        try { _onDisconnected?.Invoke(reason); }
        catch { /* consumer callback must not tear us down */ }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _state, (int)SubscriptionState.Disposed, (int)SubscriptionState.Active)
            != (int)SubscriptionState.Active)
        {
            // Already tripped or disposed; still ensure the loop and remote are torn down.
            _cts.Cancel();
            _channel.Writer.TryComplete();
            return;
        }

        _disconnectReason = HCore.Modules.Base.DisconnectReason.Disposed;
        _cts.Cancel();
        _channel.Writer.TryComplete();

        var remote = _remote;
        if (remote is not null && _client.IsConnected)
        {
            try { _client.UnsubscribeAsync(remote).GetAwaiter().GetResult(); }
            catch { /* best-effort unsubscribe */ }
        }
    }

    private static DisconnectReason MapError(string reason)
        => reason.Contains("overload", StringComparison.OrdinalIgnoreCase)
            ? HCore.Modules.Base.DisconnectReason.Overload
            : HCore.Modules.Base.DisconnectReason.HandlerException;
}
