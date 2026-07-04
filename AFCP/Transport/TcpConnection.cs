using System.Net.Sockets;

namespace AFCP.Transport;

/// <summary>
/// A <see cref="IConnection"/> over a TCP <see cref="TcpClient"/>. Both sides of
/// a link use the same type — server-accepted clients and outgoing clients are
/// indistinguishable at this layer.
/// </summary>
public sealed class TcpConnection : IConnection
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private volatile bool _closed;

    public TcpConnection(TcpClient client)
    {
        _client = client;
        // FramedTransport.WriteMessage does a length-prefix write then a
        // payload write; without NoDelay, Nagle holds the first small
        // segment for the peer's delayed ACK — ~40ms per frame.
        client.NoDelay = true;
        _stream = client.GetStream();
    }

    public bool IsConnected => !_closed && _client.Connected;

    public void Write(ReadOnlySpan<byte> data)
    {
        if (_closed) throw new InvalidOperationException("Connection is closed.");
        _stream.Write(data);
    }

    public int Read(Span<byte> buffer)
    {
        if (_closed) return 0;
        return _stream.Read(buffer);
    }

    public void ReadExactly(Span<byte> buffer, int count)
    {
        if (count > buffer.Length) throw new ArgumentException("count exceeds buffer length.", nameof(count));
        if (count == 0) return;
        var slice = buffer[..count];
        _stream.ReadExactly(slice);
    }

    public void Close()
    {
        if (_closed) return;
        _closed = true;
        try { _stream.Close(); } catch { /* ignore */ }
        try { _client.Close(); } catch { /* ignore */ }
    }

    public void Dispose()
    {
        Close();
        _stream.Dispose();
        _client.Dispose();
    }
}
