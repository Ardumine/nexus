namespace AFCP.Protocol;

/// <summary>
/// AFCP protocol message types. Each <see cref="Multiplex.Frame"/> carries one of
/// these in its <see cref="Multiplex.Frame.MessageType"/> field. The set is
/// intentionally small and path-addressed (the path-prefix mount model from
/// DATA_PLANE_DESIGN.md Part IX): remoteness is a path prefix, never a first-class
/// kernel identity.
///
/// Request/response pairs share a type code; the <see cref="Multiplex.FrameKind"/>
/// (Request vs Response vs Notify) disambiguates direction. Notify-only types are
/// marked.
/// </summary>
public static class MessageType
{
    /// <summary>Handshake. Request: <see cref="ConnectRequest"/>. Response: <see cref="ConnectResponse"/>.</summary>
    public const ushort Connect = 1;

    /// <summary>List the facets/entries a peer exposes under a path prefix. Request: <see cref="SyncRequest"/>. Response: <see cref="SyncResponse"/>.</summary>
    public const ushort Sync = 2;

    /// <summary>Snapshot-read a facet's current value. Request: <see cref="ReadRequest"/>. Response: <see cref="ReadResponse"/>.</summary>
    public const ushort Read = 3;

    /// <summary>Subscribe to push events for a facet. Request: <see cref="SubscribeRequest"/>. Response: <see cref="SubscribeResponse"/>. Subsequent frames: <see cref="EventNotify"/> (Notify).</summary>
    public const ushort Subscribe = 4;

    /// <summary>Cancel a subscription. Request: <see cref="UnsubscribeRequest"/>. Response: empty.</summary>
    public const ushort Unsubscribe = 5;

    /// <summary>Create or overwrite a file. Request: <see cref="WriteRequest"/>. Response: <see cref="WriteResponse"/>.</summary>
    public const ushort Write = 6;

    /// <summary>Create a directory (and any missing parents). Request: <see cref="MkDirRequest"/>. Response: <see cref="MkDirResponse"/>.</summary>
    public const ushort MkDir = 7;

    /// <summary>Delete a single file or empty directory. Request: <see cref="RemoveRequest"/>. Response: <see cref="RemoveResponse"/>.</summary>
    public const ushort Remove = 8;

    /// <summary>Invoke a method on a remote module instance (Layer 3 — MKCall proxy).
    /// Request: <see cref="CallRequest"/>. Response: <see cref="CallResponse"/>.</summary>
    public const ushort Call = 9;

    /// <summary>Notify-only: a pushed data frame for an active subscription.</summary>
    public const ushort Event = 100;

    /// <summary>Notify-only: the producer at a subscribed path is gone (killed/disconnected).</summary>
    public const ushort ProducerGone = 101;

    /// <summary>Notify-only: an error occurred on a subscription (e.g. breaker tripped remotely).</summary>
    public const ushort SubscriptionError = 102;
}

public enum FacetPrimitiveKind : byte
{
    Cell = 0,
    Stream = 1,
}
