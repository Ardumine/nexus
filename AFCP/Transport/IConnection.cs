namespace AFCP.Transport;

/// <summary>
/// A bidirectional, ordered byte transport (a single TCP/UART/... link). This is
/// layer 0 of the AFCP stack: raw bytes in, raw bytes out. Framing, integrity,
/// and multiplexing are layered on top by <see cref="AFCP.Streams"/> and
/// <see cref="AFCP.Multiplex"/>.
///
/// Implementations MUST be safe for a single dedicated reader thread and a single
/// dedicated writer thread (the <see cref="MultiplexedConnection"/> model); they
/// need not be safe for concurrent readers or concurrent writers.
/// </summary>
public interface IConnection : IDisposable
{
    /// <summary>Write <paramref name="data"/> to the link. Blocks until all bytes are written.</summary>
    void Write(ReadOnlySpan<byte> data);

    /// <summary>
    /// Read up to <paramref name="buffer"/>.Length bytes into <paramref name="buffer"/>.
    /// Returns the number of bytes read (0 = end of stream). Blocks until at least
    /// one byte is available or the link is closed.
    /// </summary>
    int Read(Span<byte> buffer);

    /// <summary>Read exactly <paramref name="count"/> bytes, blocking until satisfied. Throws if the link closes first.</summary>
    void ReadExactly(Span<byte> buffer, int count);

    /// <summary>Gracefully close the link. Idempotent.</summary>
    void Close();

    /// <summary>True while the link is open and usable.</summary>
    bool IsConnected { get; }
}
