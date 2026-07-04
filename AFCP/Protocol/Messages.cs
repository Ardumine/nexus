namespace AFCP.Protocol;

/// <summary>
/// A typed failure code carried by fallible responses (<see cref="SyncResponse"/>,
/// <see cref="ReadResponse"/>) so a peer can distinguish "not found" from
/// "permission denied" from "not a directory" — previously these all collapsed to
/// <c>Exists=false</c> / an empty listing (TODO.md §C7d). The serving peer maps its
/// local VFS exception to one of these; the mounting peer maps the code back to a
/// specific .NET exception (see <c>RemoteFileSystem</c>). <see cref="None"/> means
/// success. Serializes as a single byte (enum fast path).
/// </summary>
public enum AfcpErrorCode : byte
{
    /// <summary>No error — the operation succeeded.</summary>
    None = 0,
    /// <summary>The path does not exist.</summary>
    NotFound = 1,
    /// <summary>A directory operation targeted a path that exists but is a file.</summary>
    NotADirectory = 2,
    /// <summary>A file operation targeted a path that exists but is a directory.</summary>
    NotAFile = 3,
    /// <summary>The serving peer refused access (no capability model yet — reserved for a real ACL).</summary>
    PermissionDenied = 4,
    /// <summary>The target filesystem is read-only.</summary>
    ReadOnly = 5,
    /// <summary>Anything else — an unclassified server-side failure (message carries detail).</summary>
    Internal = 99,
}

// --- Connect (handshake) ---

public sealed class ConnectRequest
{
    public ushort ProtocolVersion { get; set; }
    public string PeerName { get; set; } = string.Empty;
}

public sealed class ConnectResponse
{
    public ushort ProtocolVersion { get; set; }
    public string PeerName { get; set; } = string.Empty;
    public bool Accepted { get; set; }
    public string? Error { get; set; }
}

// --- Sync (list a directory) ---

public sealed class SyncRequest
{
    /// <summary>Absolute path on the peer, e.g. <c>/</c>, <c>/proc</c>, <c>/etc</c>.</summary>
    public string Path { get; set; } = string.Empty;
}

/// <summary>One entry in a remote directory listing.</summary>
public sealed class DirEntry
{
    public string Name { get; set; } = string.Empty;
    public bool IsDirectory { get; set; }
}

public sealed class SyncResponse
{
    public DirEntry[] Entries { get; set; } = Array.Empty<DirEntry>();
    /// <summary>Failure code; <see cref="AfcpErrorCode.None"/> when the listing succeeded. A missing directory reports <see cref="AfcpErrorCode.NotFound"/> (the mount side treats that as an empty listing to preserve path traversal).</summary>
    public AfcpErrorCode Error { get; set; } = AfcpErrorCode.None;
    /// <summary>Human-readable detail for <see cref="Error"/>, or null on success.</summary>
    public string? ErrorMessage { get; set; }
}

// --- Read (file contents / facet snapshot) ---

public sealed class ReadRequest
{
    public string Path { get; set; } = string.Empty;
    /// <summary>Byte offset to start reading from (chunked reads, §C7e). 0 = start of file.</summary>
    public long Offset { get; set; }
    /// <summary>Max bytes to return in this chunk; 0 = whole file from <see cref="Offset"/> (back-compat one-shot read).</summary>
    public int MaxLength { get; set; }
}

public sealed class ReadResponse
{
    /// <summary>The file's raw bytes for the requested window (the server reads live from its VFS on every request).</summary>
    public byte[]? Data { get; set; }
    /// <summary>True if the path exists; false lets the client report "not found".</summary>
    public bool Exists { get; set; }
    /// <summary>Failure code; <see cref="AfcpErrorCode.None"/> when the read succeeded. Lets the mount side distinguish not-found / not-a-file / permission-denied instead of a bare <see cref="Exists"/>=false.</summary>
    public AfcpErrorCode Error { get; set; } = AfcpErrorCode.None;
    /// <summary>Human-readable detail for <see cref="Error"/>, or null on success.</summary>
    public string? ErrorMessage { get; set; }
    /// <summary>Total file size in bytes, so a chunked reader knows when it is done (§C7e). Valid when the read succeeded.</summary>
    public long TotalLength { get; set; }
    /// <summary>True when this chunk reaches the end of the file (§C7e).</summary>
    public bool Eof { get; set; }
}

// --- Write (create/overwrite a file) ---

public sealed class WriteRequest
{
    public string Path { get; set; } = string.Empty;
    public byte[]? Data { get; set; }
    public bool Overwrite { get; set; } = true;
    /// <summary>Byte offset to write this chunk at (chunked writes, §C7e). 0 = create/overwrite from the start (honours <see cref="Overwrite"/>); &gt;0 = seek + write a continuation chunk.</summary>
    public long Offset { get; set; }
}

public sealed class WriteResponse
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

// --- MkDir (create a directory, and any missing parents) ---

public sealed class MkDirRequest
{
    public string Path { get; set; } = string.Empty;
}

public sealed class MkDirResponse
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

// --- Remove (delete a single file or empty directory) ---

public sealed class RemoveRequest
{
    public string Path { get; set; } = string.Empty;
}

public sealed class RemoveResponse
{
    public bool Success { get; set; }
    public string? Error { get; set; }
}

// --- Subscribe (push) ---

public sealed class SubscribeRequest
{
    public string Path { get; set; } = string.Empty;
}

public sealed class SubscribeResponse
{
    public ulong SubscriptionId { get; set; }
    public bool Accepted { get; set; }
    public string? Error { get; set; }
    /// <summary>The type of the facet's values, so the subscriber can pick a deserializer.</summary>
    public string? ValueTypeFullName { get; set; }
}

public sealed class UnsubscribeRequest
{
    public ulong SubscriptionId { get; set; }
}

// --- Notify-only (push) ---

public sealed class EventNotify
{
    public ulong SubscriptionId { get; set; }
    public long Sequence { get; set; }
    public long InterFrameDelta { get; set; }
    public bool HasInterFrameDelta { get; set; }
    /// <summary>Serialized value for this frame, or null for a "no current value" tick.</summary>
    public byte[]? Data { get; set; }
    public string? ValueTypeFullName { get; set; }
}

public sealed class ProducerGoneNotify
{
    public ulong SubscriptionId { get; set; }
}

public sealed class SubscriptionErrorNotify
{
    public ulong SubscriptionId { get; set; }
    public string? Reason { get; set; }
}

// --- Call (Layer 3 — MKCall proxy) ---

/// <summary>
/// Invoke a method on a remote module instance. The path identifies the instance
/// as the SERVING peer sees it (mount prefix already stripped, e.g.
/// <c>/proc/lidar</c>). <see cref="MethodName"/> + <see cref="ParamTypeNames"/>
/// identify the method (overload-safe — no shared method-index contract, unlike
/// V2). <see cref="Args"/> rides the serializer's polymorphic <c>object[]</c>
/// path: each element carries its own runtime type tag, so mixed-type argument
/// lists serialize without a per-arg wrapper.
/// </summary>
public sealed class CallRequest
{
    /// <summary>Instance path on the serving peer, e.g. <c>/proc/lidar</c> or a bare name.</summary>
    public string InstancePath { get; set; } = string.Empty;

    public string MethodName { get; set; } = string.Empty;

    /// <summary>Assembly-qualified names of the method's declared parameter types (overload disambiguation).</summary>
    public string[] ParamTypeNames { get; set; } = Array.Empty<string>();

    /// <summary>One element per parameter, polymorphic (DerivedSerializer path). Empty for no-arg methods.</summary>
    public object?[] Args { get; set; } = Array.Empty<object?>();
}

/// <summary>
/// The outcome of a remote method call. <see cref="ReturnValue"/> is the boxed
/// return value (null for void methods, null reference returns, or failures);
/// the proxy already knows from <see cref="System.Reflection.MethodInfo.ReturnType"/>
/// whether to expect a value, so the response carries no separate void flag.
/// </summary>
public sealed class CallResponse
{
    public bool Success { get; set; }

    /// <summary><c>"Type.FullName: Message"</c> when <see cref="Success"/> is false.</summary>
    public string Error { get; set; } = string.Empty;

    /// <summary>Serialized return value (polymorphic), or null. Ignored when <see cref="Success"/> is false.</summary>
    public object? ReturnValue { get; set; }
}
