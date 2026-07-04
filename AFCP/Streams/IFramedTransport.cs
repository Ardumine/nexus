namespace AFCP.Streams;

/// <summary>
/// A message-oriented layer over a raw <see cref="Transport.IConnection"/>. Each
/// call to <see cref="WriteMessage"/> writes one self-delimiting message; each
/// call to <see cref="ReadMessage"/> blocks until one complete message arrives
/// and returns its bytes. This is layer 1 of the AFCP stack: it turns an
/// arbitrary byte stream into a sequence of discrete frames so the multiplexer
/// (layer 2) can correlate request/response pairs by id.
///
/// Implementations may add integrity (checksum), encryption, or transport
/// disguise by decorating another <see cref="IFramedTransport"/>.
/// </summary>
public interface IFramedTransport : IDisposable
{
    /// <summary>Write a single framed message. Blocks until the full frame is on the wire.</summary>
    void WriteMessage(ReadOnlySpan<byte> message);

    /// <summary>
    /// Block until a complete framed message arrives and return its payload bytes.
    /// Returns null if the transport is closed (clean shutdown). Throws on a
    /// truncated frame (broken link).
    /// </summary>
    byte[]? ReadMessage();
}
